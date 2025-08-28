import csv
import sys
import os
import pymysql

# Add parent directory to import custom modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import append_to_csv

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Duplicates/CSV_FILES"
INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Duplicates/CSV_FILES/input_STR_invoice.csv"

def validateSTRDuplicate(input_csv):
    with open(input_csv, 'r') as file:
        reader = csv.DictReader(file)   # ✅ FIXED

        for row in reader:
            tenant = row["skull_namespace"]
            invoiceNo = row["invoice_no"]
            status = "StockHidden"
            purchaseType = ["ICSReturn", "StockTransferReturn"]

            print(f"Processing invoice {invoiceNo} for tenant {tenant}")

            connection = create_db_connection(tenant)
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            try:
                # ✅ Proper IN clause handling
                placeholders = ','.join(['%s'] * len(purchaseType))
                query = f"""
                    SELECT * FROM {tenant}.inward_invoice 
                    WHERE invoice_no = %s AND purchase_type IN ({placeholders}) AND status not in ('DELETED' , 'CANCELLED')
                """
                cursor.execute(query, [invoiceNo] + purchaseType)
                invoices = cursor.fetchall()

                # Filter invoices by status
                filtered_invoices = [inv for inv in invoices if inv["status"] == status]

                if len(invoices) > 1:
                    if len(filtered_invoices) == len(invoices):
                        # Case 1: all invoices have StockHidden status
                        for inv in invoices[:-1]:   # ✅ keep last one
                            cancelInvoiceQuery = f"UPDATE {tenant}.inward_invoice SET updated_on = NOW(), status = 'DELETED' WHERE id = {inv['id']} AND status = '{status}';"
                            cancelGatepassQuery = f"UPDATE {tenant}.gatepass_invoice SET updated_on = NOW() , status = 'CANCELLED' WHERE gatepass_id = {inv['gatepass_id']} AND status = 'CREATED';"
                            append_to_csv(
                                "update_STR_invoice.csv",
                                ["tenant", "invoice_no", "invoice_status", "cancelInvoiceQuery", "cancelGatepassQuery"],
                                [tenant, invoiceNo, inv['status'], cancelInvoiceQuery, cancelGatepassQuery],
                                OUTPUT_DIR,
                                False
                            )
                    else:
                        # Case 2: only some invoices are StockHidden
                        for inv in filtered_invoices:
                            cancelInvoiceQuery = f"UPDATE {tenant}.inward_invoice SET updated_on = NOW(), status = 'DELETED' WHERE id = {inv['id']} AND status = '{status}';"
                            cancelGatepassQuery = f"UPDATE {tenant}.gatepass_invoice SET updated_on = NOW() , status = 'CANCELLED' WHERE gatepass_id = {inv['gatepass_id']} AND status = 'CREATED';"
                            append_to_csv(
                                "update_STR_invoice.csv",
                                ["tenant", "invoice_no", "invoice_status", "cancelInvoiceQuery", "cancelGatepassQuery"],
                                [tenant, invoiceNo, inv['status'], cancelInvoiceQuery, cancelGatepassQuery],
                                OUTPUT_DIR,
                                False
                            )
                else:
                    print(f"Invoice {invoiceNo} is not a duplicate for tenant {tenant}")

            finally:
                cursor.close()
                connection.close()

validateSTRDuplicate(INPUT_CSV)
