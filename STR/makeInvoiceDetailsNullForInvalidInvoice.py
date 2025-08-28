import sys
import os
import csv
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import append_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES/purchase_issues_with_invalid_invoice_for_non_dc_v20.csv"
OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES"

alreadySeen = set()


def getPurchaseIssues(db_name, ids):
    connection = create_db_connection(db_name)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    placeholders = ','.join(['%s'] * len(ids))
    cursor.execute(
        f"SELECT * FROM purchase_issue WHERE id IN ({placeholders})",
        ids
    )
    purchaseIssues = cursor.fetchall()
    cursor.close()
    connection.close()
    return purchaseIssues


def validateInvoice(db_name, invoiceId, pdi):
    if invoiceId is None:
        return True
    connection = create_db_connection(db_name)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        """
        SELECT `purchase_type`, partner_detail_id
        FROM inward_invoice
        WHERE id = %s
        """,
        (invoiceId,)
    )
    invoice = cursor.fetchone()
    cursor.close()
    connection.close()
    if invoice is None:
        return False
    return invoice["purchase_type"] in ("ICS", "StockTransfer") and invoice["partner_detail_id"] == pdi


def hasInvoiceInPurchaseIssueInvoice(db_name, purchaseIssueId):
    connection = create_db_connection(db_name)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "SELECT * FROM purchase_issue_invoice WHERE purchase_issue_id = %s",
        (purchaseIssueId,)
    )
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result is not None


def chunks(lst, n):
    """Yield successive n-sized chunks from list"""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def process_batch(db_name, ids_batch):
    """Process a batch of IDs for a tenant"""
    data = []
    purchaseIssues = getPurchaseIssues(db_name, ids_batch)
    if purchaseIssues:
        for purchaseIssue in purchaseIssues:
            invoiceId = purchaseIssue["invoice_id"]
            isValidInvoice = validateInvoice(db_name, invoiceId, purchaseIssue["partner_detail_id"])
            if not isValidInvoice:
                invoice_no = purchaseIssue["invoice_no"]

                updateInvoiceDetailsQuery = (
                    f"UPDATE {db_name}.purchase_issue "
                    f"SET updated_on = NOW(), invoice_id = NULL, invoice_no = NULL, invoice_tenant = NULL "
                    f"WHERE id = {purchaseIssue['id']} "
                    f"AND invoice_id = {invoiceId} "
                    f"AND invoice_no = '{invoice_no}';"
                )

                updateUnifiedVendorQuery = (
                    f"UPDATE unified_vendor_returnable_item "
                    f"SET quantity = 0, updated_on = NOW(), status = 'CANCELLED' "
                    f"WHERE reference_id = {purchaseIssue['id']} "
                    f"AND reference_type = 'PURCHASE_RETURN' "
                    f"AND partner_detail_id = {purchaseIssue['partner_detail_id']} "
                    f"AND tenant = '{db_name}';"
                )

                deletePurchaseIssueInvoiceQuery = ""
                if hasInvoiceInPurchaseIssueInvoice(db_name, purchaseIssue['id']):
                    deletePurchaseIssueInvoiceQuery = (
                        f"DELETE FROM {db_name}.purchase_issue_invoice "
                        f"WHERE purchase_issue_id = {purchaseIssue['id']};"
                    )

                data.append([
                    purchaseIssue['id'],
                    db_name,
                    updateInvoiceDetailsQuery,
                    updateUnifiedVendorQuery,
                    deletePurchaseIssueInvoiceQuery
                ])
    return data


def makeInvoiceDetailsNullFromPR(batch_size=500, max_workers=10):
    tenantToRowMap = {}

    # Group IDs by tenant
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            db_name = row["source_tenant"]
            if db_name in ('th303', 'th438', 'th997'):
                continue
            pid = row["purchase_issue_id"]

            if db_name not in tenantToRowMap:
                tenantToRowMap[db_name] = []
            tenantToRowMap[db_name].append(pid)

    # Submit all tenant batches as jobs to a global pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for db_name, ids in tenantToRowMap.items():
            for batch in chunks(ids, batch_size):
                futures.append(executor.submit(process_batch, db_name, batch))

        for f in as_completed(futures):
            try:
                result = f.result()
                if result:  # only write non-empty
                    append_to_csv(
                        "makeInvoiceDetailsNullForNonDC_Output_v20.csv",
                        ["purchase_issue_id", "source_tenant", "updateInvoiceDetailsQuery",
                         "updateUnifiedVendorQuery", "deletePurchaseIssueInvoiceQuery"],
                        result,
                        OUTPUT_DIR
                    )
            except Exception as e:
                print(f"Error in batch: {e}")

    print("total already seen:", len(alreadySeen))
    print("✅ Done! All chunks processed & written incrementally.")


if __name__ == "__main__":
    makeInvoiceDetailsNullFromPR(batch_size=500, max_workers=10)
