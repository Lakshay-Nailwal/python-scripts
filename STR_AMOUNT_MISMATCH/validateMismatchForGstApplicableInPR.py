import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv , append_to_csv

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_AMOUNT_MISMATCH/CSV_FILES"
INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_AMOUNT_MISMATCH/CSV_FILES/mismatchInput.csv"

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

def process_csv():
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            source_debit_note_number = row["debit_note_number"]
            dest_tenant = row["dest_tenant"]
            ucode = row["ucode"].zfill(6)
            batch = row["batch"]
            amount = float(row["total_return_amt"])
            quantity = float(row["total_return_qty"])

            inwardInvoiceItem = getInwardInvoiceItem(source_debit_note_number, dest_tenant, ucode, batch)

            if not inwardInvoiceItem:
                inwardInvoiceItem = [{"amount": 0.0, "quantity": 0.0}]

            deltaInAmt = amount - float(inwardInvoiceItem[0]["amount"])
            deltaInQty = quantity - float(inwardInvoiceItem[0]["quantity"])

            append_to_csv(
                "mismatchOutput.csv",
                [
                    "debit_note_number", "dest_tenant", "ucode", "batch",
                    "amountInPR", "quantityInPR", "amountInInvoice", "quantityInInvoice",
                    "deltaInAmt", "deltaInQty"
                ],
                [
                    source_debit_note_number,
                     dest_tenant,
                   ucode,
                    batch,
                    amount,
                    quantity,
                    inwardInvoiceItem[0]["amount"],
                     inwardInvoiceItem[0]["quantity"],
                     deltaInAmt,
                     deltaInQty,
                ],
                OUTPUT_DIR,
                False
            )



def getInwardInvoiceItem(debit_note_number, dest_tenant , ucode , batch):
    return query_db(dest_tenant, "SELECT SUM(iii.total_quantity) as quantity, SUM(iii.net_amount) as amount FROM inward_invoice ii join inward_invoice_item iii on ii.id = iii.invoice_id WHERE invoice_no = %s and iii.code = %s and iii.batch = %s and ii.status not in ('CANCELLED', 'DELETED') group by iii.code, iii.batch", (debit_note_number, ucode, batch))

process_csv()