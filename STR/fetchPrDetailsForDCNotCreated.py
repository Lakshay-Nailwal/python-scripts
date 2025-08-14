import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pdi import pdiToTenantMap
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES"

INPUT_FILE = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES/dest_invoice_not_created_output_STR.csv"

def fetchPurchaseIssuesForDC(debit_note_number, tenant):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        query = f"""
            SELECT pi.id , pi.partner_detail_id , pi.tray_id , pi.invoice_id , pi.invoice_no , pi.invoice_sequence_type , pi.pr_type , pi.invoice_date , pi.invoice_tenant , pi.status FROM purchase_issue pi
            WHERE debit_note_number = %s
            AND pr_type <> 'REGULAR_EASYSOL'
            AND invoice_date > '2025-05-28'
        """
        cursor.execute(query, (debit_note_number,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching purchase issues for tenant {tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def fetchPurchaseIssueItemsForPurchaseIssues(purchaseIssueIds, tenant):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        placeholders = ','.join(['%s'] * len(purchaseIssueIds))

        query = f"""
            SELECT id , ucode , batch , assigned_bin , return_quantity , amount , pre_purchase_issue_id , purchase_issue_id FROM purchase_issue_item
            WHERE purchase_issue_id IN ({placeholders})
        """
        cursor.execute(query, purchaseIssueIds)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching purchase issue items for tenant {tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def checkIfUcodesExistInDest(distinctUcodes, dest_tenant):
    if(len(distinctUcodes) == 0):
        return []
    try:
        conn = create_db_connection(dest_tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        placeholders = ','.join(['%s'] * len(distinctUcodes))
        print("distinctUcodes : ", distinctUcodes)

        query = f"""
            SELECT DISTINCT code FROM inward_invoice_item
            WHERE code IN ({placeholders})
        """
        cursor.execute(query, list(distinctUcodes))

        return cursor.fetchall()
    except Exception as e:
        print(f"Error checking if ucodes exist in dest for tenant {dest_tenant}: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def validateInvoiceWithConnection(cursor , db_name, invoiceId, pdi):
    if invoiceId is None:
        return True
    cursor.execute(
        """
        SELECT `purchase_type`, partner_detail_id
        FROM inward_invoice
        WHERE id = %s
        """,
        (invoiceId,)
    )
    invoice = cursor.fetchone()
    if invoice is None:
        return False
    return invoice["purchase_type"] in ("ICS", "StockTransfer") and invoice["partner_detail_id"] == pdi


csvHeaderForDcWithZeroItems = ["source_debit_note_number", "dest_tenant", "source_tenant"]
csvHeaderForPurchaseIssueItemsWithUcodeMissingInDest = ["source_debit_note_number", "dest_tenant", "source_tenant", "purchase_issue_item_id", "ucode", "batch", "assigned_bin", "return_qunatity", "amount", "pre_purchase_issue_id", "purchase_issue_id", "purchase_issue_status"]
csvHeaderForPurchaseIssuesWithInvalidInvoice = ["source_debit_note_number", "dest_tenant", "source_tenant", "purchase_issue_id", "invoice_id", "invoice_no", "invoice_sequence_type", "pr_type", "invoice_date", "invoice_tenant", "is_invoice_tenant_same" , "purchase_issue_status"]           

def fetchPrDetailsForDCNotCreated():
    with open(INPUT_FILE, 'r') as file:
        reader = csv.reader(file)
        count = 0
        for row in reader:
            if(count == 0):
                # skip header
                count += 1
                continue

            source_debit_note_number = row[0]
            dest_tenant = row[1]
            source_tenant = row[2]

            print("processing source_debit_note_number : ", source_debit_note_number , "dest_tenant : ", dest_tenant , "source_tenant : ", source_tenant)

            purchaseIssues = fetchPurchaseIssuesForDC(source_debit_note_number, source_tenant)
            if(len(purchaseIssues) == 0): continue

            purchaseIssueIds = [pi['id'] for pi in purchaseIssues]

            purchaseIssueItems = fetchPurchaseIssueItemsForPurchaseIssues(purchaseIssueIds, source_tenant)

            if(len(purchaseIssueItems) == 0):
                row_for_csv = [[source_debit_note_number, dest_tenant, source_tenant]]
                append_to_csv("dc_with_zero_items.csv",csvHeaderForDcWithZeroItems,row_for_csv, OUTPUT_DIR, False)
                continue

            conn = create_db_connection(source_tenant)
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            distinctUcodes = set([item['ucode'].zfill(6) for item in purchaseIssueItems])
            print("distinctUcodes size : ", len(distinctUcodes))

            validUcodesInDest = checkIfUcodesExistInDest(distinctUcodes, dest_tenant)
            validUcodes = [item['code'] for item in validUcodesInDest]

            purchaseIssueIdMapToStatus = {pi['id']: pi['status'] for pi in purchaseIssues}

            for item in purchaseIssueItems:
                if(item['ucode'].zfill(6) not in validUcodes):
                    row_for_csv = [[source_debit_note_number, dest_tenant, source_tenant, item['id'], item['ucode'], item['batch'], item['assigned_bin'], item['return_quantity'], item['amount'], item['pre_purchase_issue_id'], item['purchase_issue_id'] , purchaseIssueIdMapToStatus[item['purchase_issue_id']]]]
                    append_to_csv("purchase_issue_items_with_ucode_missing_in_dest.csv",csvHeaderForPurchaseIssueItemsWithUcodeMissingInDest,row_for_csv, OUTPUT_DIR, False)

            for pi in purchaseIssues:
                invoice_id = pi['invoice_id']
                pdi = pi['partner_detail_id']
                isInvoiceValid = validateInvoiceWithConnection(cursor, source_tenant, invoice_id, pdi)
                if(isInvoiceValid == False):
                    isInvoiceTenantSame = pi['invoice_tenant'] == source_tenant
                    row_for_csv = [[source_debit_note_number, dest_tenant, source_tenant, pi['id'], pi['invoice_id'], pi['invoice_no'], pi['invoice_sequence_type'], pi['pr_type'], pi['invoice_date'], pi['invoice_tenant'] , isInvoiceTenantSame , pi['status']]]
                    append_to_csv("purchase_issues_with_invalid_invoice.csv",csvHeaderForPurchaseIssuesWithInvalidInvoice,row_for_csv, OUTPUT_DIR, False)
            cursor.close()
            conn.close()


if __name__ == "__main__":
    fetchPrDetailsForDCNotCreated()