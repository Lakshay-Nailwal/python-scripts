import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv , append_to_csv
from getAllWarehouse import getAllWarehouse

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_AMOUNT_MISMATCH/CSV_FILES"

QUERY = """
    SELECT DISTINCT
       ii.id                AS invoice_id,
       gi.interstate,
       ii.status,
       ii.invoice_date,
       ii.created_on,
       ii.invoice_no,
       iii.code,
       iii.batch,
       iii.cgst,
       iii.sgst,
       iii.igst
FROM   inward_invoice ii
JOIN   inward_invoice_item iii
       ON iii.invoice_id = ii.id
JOIN   gatepass_invoice gi
       ON gi.gatepass_id = ii.gatepass_id
      AND gi.no = ii.invoice_no
WHERE  ii.purchase_type IN ('ICSReturn', 'StockTransferReturn')
  AND  ii.status NOT IN ('CANCELLED', 'DELETED')
  AND  ii.created_on >= '2025-05-28'
  AND  gi.interstate = 1
  AND  iii.igst = 0.0
  AND  iii.cgst <> 0.0
  AND  gi.status NOT IN ('CANCELLED')
"""

def query_db(db_name, query, params=None, dict_cursor=True):
    """Run a DB query and return results."""
    try:
        db_connection = create_db_connection(db_name)
        cursor_type = pymysql.cursors.DictCursor if dict_cursor else None
        cursor = db_connection.cursor(cursor_type)
        cursor.execute(query, params or ())
        return cursor.fetchall()
    except Exception as e:
        print("Error in query_db: ", e)
        return []
    finally:
        cursor.close()
        db_connection.close()

def process_tenant(tenant):
    inward_invoices = query_db(tenant, QUERY)
    print("Processing tenant: ", tenant , " with count: ", len(inward_invoices))
    for inward_invoice in inward_invoices:
        append_to_csv("amtMismatchForSameQtyInDest.csv", ["tenant","invoice_id", "interstate", "status", "invoice_date", "created_on", "invoice_no", "ucode", "batch", "cgst", "sgst", "igst"], [[tenant,   inward_invoice["invoice_id"], inward_invoice["interstate"], inward_invoice["status"], inward_invoice["invoice_date"], inward_invoice["created_on"], inward_invoice["invoice_no"], inward_invoice["code"], inward_invoice["batch"], inward_invoice["cgst"], inward_invoice["sgst"], inward_invoice["igst"]]], OUTPUT_DIR, False)

if __name__ == "__main__":

    tenants = getAllWarehouse()
    for tenant in tenants:
        if(tenant in ['th303' , 'th438' , 'th997']):
            continue
        process_tenant(tenant)