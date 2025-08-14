import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Created/CSV_FILES/cancelledInvoiceNumbers_STR.csv"
OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Created/CSV_FILES"

alreadySeen = set()

def getPurchaseIssues(db_name, source_debit_note_number):
    connection = create_db_connection(db_name)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "SELECT * FROM purchase_issue WHERE debit_note_number = %s",
        (source_debit_note_number,)
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

def makeInvoiceDetailsNullFromPR():
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        data = []
        for row in reader:
            db_name = row["source_tenant"]
            source_debit_note_number = row["source_debit_note_number"]

            # Track by (tenant, debit note) to avoid skipping valid cases
            seen_key = (db_name, source_debit_note_number)
            if seen_key in alreadySeen:
                continue
            alreadySeen.add(seen_key)

            print("tenant:", db_name, "| source_debit_note_number:", source_debit_note_number)

            purchaseIssues = getPurchaseIssues(db_name, source_debit_note_number)
            if purchaseIssues:
                for purchaseIssue in purchaseIssues:
                    invoiceId = purchaseIssue["invoice_id"]
                    isValidInvoice = validateInvoice(db_name, invoiceId, purchaseIssue["partner_detail_id"])
                    if isValidInvoice == False:
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
                            f"AND reference_type = 'PURCHASE_RETURN' AND partner_detail_id = {purchaseIssue['partner_detail_id']} AND tenant = '{db_name}';"
                        )

                        deletePurchaseIssueInvoiceQuery = (
                            f"DELETE FROM {db_name}.purchase_issue_invoice "
                            f"WHERE purchase_issue_id = {purchaseIssue['id']} "
                            f"AND invoice_id = {invoiceId} "
                            f"AND invoice_no = '{invoice_no}';"
                        )

                        data.append([
                            source_debit_note_number,
                            db_name,
                            row["dest_tenant"],
                            updateInvoiceDetailsQuery,
                            updateUnifiedVendorQuery,
                            deletePurchaseIssueInvoiceQuery
                        ])

    save_to_csv(
        "makeInvoiceDetailsNullFromPR_Output.csv",
        ["source_debit_note_number", "source_tenant", "dest_tenant", "updateInvoiceDetailsQuery", "updateUnifiedVendorQuery", "deletePurchaseIssueInvoiceQuery"],
        data,
        OUTPUT_DIR
    )

    print("total already seen:", len(alreadySeen))

makeInvoiceDetailsNullFromPR()
