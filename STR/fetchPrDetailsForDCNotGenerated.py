import sys
import os
import csv
import pymysql
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pdi import pdiToTenantMap
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES"

# Thread lock for safe CSV writes
csv_lock = threading.Lock()

# -------------------------------
# DB Queries
# -------------------------------

def fetchPurchaseIssues(tenant, pdis):
    if not pdis:
        return []
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ','.join(['%s'] * len(pdis))
        query = f"""
            SELECT id, partner_detail_id, tray_id, invoice_id, invoice_no,
                   invoice_sequence_type, pr_type, invoice_date, invoice_tenant, status
            FROM purchase_issue
            WHERE debit_note_number IS NULL
              AND pr_type <> 'REGULAR_EASYSOL'
              AND status not in ('cancelled', 'DELETED')
              AND partner_detail_id IN ({placeholders})
              AND created_on > '2025-03-01'
        """
        cursor.execute(query, tuple(pdis))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching purchase issues for tenant {tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def fetchPurchaseIssueItemsForPurchaseIssues(purchaseIssueIds, tenant):
    if not purchaseIssueIds:
        return []
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ','.join(['%s'] * len(purchaseIssueIds))
        query = f"""
            SELECT id, ucode, batch, assigned_bin, return_quantity,
                   amount, pre_purchase_issue_id, purchase_issue_id
            FROM purchase_issue_item
            WHERE purchase_issue_id IN ({placeholders})
        """
        cursor.execute(query, tuple(purchaseIssueIds))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching purchase issue items for tenant {tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def checkIfUcodesExistInDest(distinctUcodes, dest_tenant):
    if not distinctUcodes:
        return set()
    try:
        conn = create_db_connection(dest_tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ','.join(['%s'] * len(distinctUcodes))
        query = f"""
            SELECT DISTINCT code
            FROM inward_invoice_item
            WHERE code IN ({placeholders})
        """
        cursor.execute(query, tuple(distinctUcodes))
        return {row['code'] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error checking ucodes in dest tenant {dest_tenant}: {e}")
        return set()
    finally:
        cursor.close()
        conn.close()

def validateInvoiceWithConnection(cursor, invoiceId, pdi):
    if invoiceId is None:
        return True
    cursor.execute("""
        SELECT purchase_type, partner_detail_id
        FROM inward_invoice
        WHERE id = %s
    """, (invoiceId,))
    invoice = cursor.fetchone()
    if not invoice:
        return False
    return (
        invoice["purchase_type"] in ("ICS", "StockTransfer") and
        str(invoice["partner_detail_id"]) == str(pdi)
    )

# -------------------------------
# CSV Headers
# -------------------------------

csvHeaderForPurchaseIssueItemsWithUcodeMissingInDestForNonDC = [
    "tenant", "purchase_issue_item_id", "ucode", "batch", "assigned_bin", "return_quantity",
    "amount", "pre_purchase_issue_id", "purchase_issue_id", "purchase_issue_status"
]

csvHeaderForPurchaseIssuesWithInvalidInvoiceForNonDC = [
    "dest_tenant", "source_tenant", "purchase_issue_id", "invoice_id", "invoice_no",
    "pr_type", "invoice_tenant", "is_invoice_tenant_same", "purchase_issue_status"
]

# -------------------------------
# Threaded Processing Functions
# -------------------------------

def process_chunk(chunk, tenant, purchaseIssueIdToPdi, purchaseIssueIdToStatus):
    purchaseIssueItems = fetchPurchaseIssueItemsForPurchaseIssues(chunk, tenant)

    # Build PDI → Ucodes mapping
    pdiToDistinctUcodes = defaultdict(set)
    for item in purchaseIssueItems:
        pdi = purchaseIssueIdToPdi[item['purchase_issue_id']]
        pdiToDistinctUcodes[pdi].add(item['ucode'].zfill(6))

    # Fetch valid ucodes per PDI
    pdiToValidUcodes = {}
    for pdi, ucodes in pdiToDistinctUcodes.items():
        if pdi in pdiToTenantMap:
            pdiToValidUcodes[pdi] = checkIfUcodesExistInDest(ucodes, pdiToTenantMap[pdi])

    # Write missing ucode rows to CSV
    for item in purchaseIssueItems:
        pdi = purchaseIssueIdToPdi[item['purchase_issue_id']]
        if pdi in pdiToValidUcodes and item['ucode'].zfill(6) not in pdiToValidUcodes[pdi]:
            row = [[
                tenant,
                item['id'], item['ucode'], item['batch'], item['assigned_bin'],
                item['return_quantity'], item['amount'], item['pre_purchase_issue_id'],
                item['purchase_issue_id'], purchaseIssueIdToStatus[item['purchase_issue_id']]
            ]]
            with csv_lock:
                append_to_csv(
                    "purchase_issue_items_with_ucode_missing_in_dest_for_non_dc.csv",
                    csvHeaderForPurchaseIssueItemsWithUcodeMissingInDestForNonDC,
                    row, OUTPUT_DIR, False
                )

def validate_invoice(pi, tenant):
    pdi = str(pi['partner_detail_id'])
    if pdi not in pdiToTenantMap:
        return
    dest_tenant = pdiToTenantMap[pdi]
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if not validateInvoiceWithConnection(cursor, pi['invoice_id'], pdi):
            isInvoiceTenantSame = pi['invoice_tenant'] == tenant
            row = [[
                dest_tenant, tenant, pi['id'], pi['invoice_id'], pi['invoice_no'],
                pi['pr_type'], pi['invoice_tenant'], isInvoiceTenantSame, pi['status']
            ]]
            with csv_lock:
                append_to_csv(
                    "purchase_issues_with_invalid_invoice_for_non_dc.csv",
                    csvHeaderForPurchaseIssuesWithInvalidInvoiceForNonDC,
                    row, OUTPUT_DIR, False
                )
    except Exception as e:
        print(f"Error validating invoice for PI {pi['id']} in tenant {tenant}: {e}")
    finally:
        cursor.close()
        conn.close()

# -------------------------------
# Main Process
# -------------------------------

def fetchPrDetailsForDCNotGenerated():
    warehouse_list = getAllWarehouse()

    for tenant_info in warehouse_list:
        tenant = tenant_info[0] if isinstance(tenant_info, (list, tuple)) else str(tenant_info)
        print(f"Processing tenant: {tenant}")
        if tenant == 'th438' or tenant == 'th997':
            continue

        pdis = list(pdiToTenantMap.keys())
        purchaseIssues = fetchPurchaseIssues(tenant, pdis)
        if not purchaseIssues:
            continue

        purchaseIssueIds = [pi['id'] for pi in purchaseIssues]
        purchaseIssueIdToPdi = {pi['id']: str(pi['partner_detail_id']) for pi in purchaseIssues}
        purchaseIssueIdToStatus = {pi['id']: pi['status'] for pi in purchaseIssues}

        # Step 3: Process chunks in threads
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for i in range(0, len(purchaseIssueIds), 100):
                chunk = purchaseIssueIds[i:i+100]
                futures.append(executor.submit(process_chunk, chunk, tenant, purchaseIssueIdToPdi, purchaseIssueIdToStatus))
            for f in as_completed(futures):
                f.result()

        # Step 4: Validate invoices in threads
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for pi in purchaseIssues:
                if pi['invoice_id'] is not None:
                    futures.append(executor.submit(validate_invoice, pi, tenant))
            for f in as_completed(futures):
                f.result()

if __name__ == "__main__":
    fetchPrDetailsForDCNotGenerated()
