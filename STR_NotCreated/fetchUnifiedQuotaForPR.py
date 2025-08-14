import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/CSV_FILES/dest_invioce_not_created_output_STR.csv"

# ---------------------------
# DB Utility Functions
# ---------------------------
def query_db(db_name, query, params=None, dict_cursor=True):
    """Run a DB query and return results."""
    db_connection = create_db_connection(db_name)
    cursor_type = pymysql.cursors.DictCursor if dict_cursor else None
    cursor = db_connection.cursor(cursor_type)
    try:
        cursor.execute(query, params or ())
        return cursor.fetchall()
    finally:
        cursor.close()
        db_connection.close()

# ---------------------------
# Fetch Functions
# ---------------------------
def fetchInvoiceDetails(invoiceId, db_name):
    return query_db(
        db_name,
        "SELECT id, partner_detail_id, status, purchase_type FROM inward_invoice WHERE id = %s",
        (invoiceId,)
    )

def fetchPurchaseIssueDetails(debitNoteNumber, db_name):
    return query_db(
        db_name,
        """SELECT id, invoice_id, invoice_no, debit_note_number,
                  invoice_sequence_type, tray_id, invoice_date, invoice_tenant,
                  partner_detail_id, child_tenant_partner_detail_id
           FROM purchase_issue 
           WHERE debit_note_number = %s""",
        (debitNoteNumber,)
    )

def fetchPurchaseIssueItems(purchaseIssueId, db_name):
    return query_db(
        db_name,
        "SELECT ucode, batch, return_quantity, pre_purchase_issue_id FROM purchase_issue_item WHERE purchase_issue_id = %s",
        (purchaseIssueId,)
    )

def fetchUnifiedVendorReturnableItem(source_tenant, pdi, ucode, batch):
    SQL = """SELECT 
                v.invoice_id,
                SUM(v.multiplier * v.quantity) AS total_qty,
                v.invoice_no,
                MAX(v.epr) AS max_epr,
                v.tenant
             FROM unified_vendor_returnable_item v
             WHERE v.tenant = %s
             AND v.partner_detail_id = %s
             AND v.ucode = %s
             AND v.batch = %s
             AND v.invoice_id IS NOT NULL
             AND v.status NOT IN ('CANCELLED')
             GROUP BY v.invoice_id, v.invoice_no, v.tenant
             HAVING SUM(v.multiplier * v.quantity) > 0"""
    return query_db("mercury", SQL, (source_tenant, pdi, ucode, batch), dict_cursor=True)

def validQuota(quota, source_tenant):
    invoiceDetails = fetchInvoiceDetails(quota["invoice_id"], source_tenant)
    if invoiceDetails:
        return invoiceDetails[0]["purchase_type"] in ("ICS", "StockTransfer")
    return False

def purchaseIssueInvoice(purchase_issue_id, source_tenant):
    return query_db(
        source_tenant,
        "SELECT id FROM purchase_issue_invoice WHERE purchase_issue_id = %s",
        (purchase_issue_id,)
    )

def createBackupForPurchaseIssueInvoice(ids, source_tenant):
    return query_db(
        source_tenant,
        "SELECT * FROM purchase_issue_invoice WHERE id in %s",
        (ids,)
    )

# ---------------------------
# Main Processing
# ---------------------------
def process_csv():
    dataForV2 = []
    OUTPUT_CSV_V2 = "dest_invoice_not_created_debugging_required_v2.csv"
    csv_header_v2 = [
        "source_debit_note_number", "dest_tenant", "source_tenant",
        "id_in_inward_invoice", "partner_detail_id_in_inward_invoice", "status_in_inward_invoice", "purchase_type_in_inward_invoice",
        "purchase_issue_id", "invoice_id", "invoice_no", "debit_note_number", "invoice_sequence_type", "tray_id", "invoice_date", "invoice_tenant",
        "partner_detail_id_in_purchase_issue", "child_tenant_partner_detail_id", "isInvoiceTenantDifferent", "isST_ICS_Condition_Cause",
        "updated_invoice_id", "updated_invoice_no", "updated_invoice_tenant", "update_Query", "delete_Query", "update_uni_vendor_query"
    ]

    columns = [
        "id",
        "invoice_id",
        "invoice_no",
        "purchase_issue_id",
        "quantity",
        "created_on",
        "updated_on",
        "created_by",
        "updated_by",
        "purchase_issue_item_id",
        "tenant"
    ]

    backupData = []
    dataToUpdatePR = []

    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            source_debit_note_number = row["source_debit_note_number"]
            source_tenant = row["source_tenant"]
            dest_tenant = row["dest_tenant"]

            purchaseIssueDetails = fetchPurchaseIssueDetails(source_debit_note_number, source_tenant)
            if not purchaseIssueDetails:
                continue

            for purchaseIssue in purchaseIssueDetails:
                items = fetchPurchaseIssueItems(purchaseIssue["id"], source_tenant)
                if not items:
                    updatePrQuery = f"""
                        UPDATE {source_tenant}.purchase_issue set updated_on = NOW() , status = 'cancelled' where id = {purchaseIssue["id"]} and 
                    """
                    continue

                isInvoiceTenantDifferent = purchaseIssue["invoice_tenant"] and purchaseIssue["invoice_tenant"] != source_tenant

                invoiceDetails = fetchInvoiceDetails(purchaseIssue["invoice_id"], source_tenant)
                invoice_id = invoice_partner_detail_id = invoice_status = invoice_purchase_type = ""

                if invoiceDetails:
                    invoice_id = invoiceDetails[0]["id"]
                    invoice_partner_detail_id = invoiceDetails[0]["partner_detail_id"]
                    invoice_status = invoiceDetails[0]["status"]
                    invoice_purchase_type = invoiceDetails[0]["purchase_type"]

                isST_ICS_Condition_Cause = (
                    invoiceDetails
                    and invoiceDetails[0]["purchase_type"] in ["ICS", "StockTransfer"]
                    and invoiceDetails[0]["partner_detail_id"] != purchaseIssue["partner_detail_id"]
                )

                update_Query = f"""
                UPDATE {source_tenant}.purchase_issue
                SET invoice_id = NULL, invoice_no = NULL, invoice_tenant = NULL, updated_on = NOW()
                WHERE debit_note_number = '{source_debit_note_number}' and invoice_id = {purchaseIssue["invoice_id"]} and invoice_no = '{purchaseIssue["invoice_no"]}'
                """

                update_uni_vendor_query = f"""
                UPDATE unified_vendor_returnable_item
                SET quantity = 0 , updated_on = NOW(), status = 'CANCELLED'
                WHERE reference_id = {purchaseIssue["id"]} and reference_type = 'PURCHASE_RETURN' and tenant = '{source_tenant}' and partner_detail_id = {purchaseIssue["partner_detail_id"]}
                """

                purchaseIssueInvoices = purchaseIssueInvoice(purchaseIssue["id"], source_tenant)
                ids = [invoice["id"] for invoice in purchaseIssueInvoices]
                delete_Query = f"""
                DELETE FROM {source_tenant}.purchase_issue_invoice
                WHERE id in {ids}
                """ if ids else ""

                if ids:
                    backup = createBackupForPurchaseIssueInvoice(ids, source_tenant)
                    for b in backup:
                        b["tenant"] = source_tenant
                    backupData.extend(backup)

                dataForV2.append([
                    source_debit_note_number, dest_tenant, source_tenant,
                    invoice_id, invoice_partner_detail_id, invoice_status, invoice_purchase_type,
                    purchaseIssue["id"], purchaseIssue["invoice_id"], purchaseIssue["invoice_no"],
                    purchaseIssue["debit_note_number"], purchaseIssue["invoice_sequence_type"], purchaseIssue["tray_id"],
                    purchaseIssue["invoice_date"], purchaseIssue["invoice_tenant"], purchaseIssue["partner_detail_id"],
                    purchaseIssue["child_tenant_partner_detail_id"], isInvoiceTenantDifferent, isST_ICS_Condition_Cause,
                    update_Query, delete_Query, update_uni_vendor_query
                ])

    # Convert backupData (list of dicts) into list of lists for CSV
    backupDataRows = [[row.get(col, "") for col in columns] for row in backupData]

    save_to_csv(OUTPUT_CSV_V2, csv_header_v2, dataForV2)
    save_to_csv("backup_purchase_issue_invoice.csv", columns, backupDataRows)


# ---------------------------
# Main Execution
# ---------------------------
if __name__ == "__main__":
    process_csv()
