import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_DN/CSV_FILES"
INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_DN/CSV_FILES/GDN_ret_CN_null_arsenal.csv"

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

def checkIfDnCreatedOnVault(invoiceId , tenant):
    return query_db(
        'vault',
        "SELECT 1 FROM debitnote WHERE return_order_id = %s and tenant = %s and note_type in ('ICS_RETURN' , 'ST_RETURN')",
        (invoiceId,tenant)
    )

def process_csv():

    seenInvoiceNum = []
    data = []
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            invoiceId = int(float(row['transactionno']))
            invoiceNo = row['invoice_no']
            tenant = row['warehouseid']

            if(invoiceNo in seenInvoiceNum): continue

            seenInvoiceNum.append(invoiceNo)

            print(f"Processing --> Invoice : {invoiceNo} | tenant : {tenant}")

            try:
                if checkIfDnCreatedOnVault(invoiceId, tenant):
                    print(f"✅ DN already created for invoiceNo {invoiceNo} and tenant {tenant}")
                else:
                    data.append([invoiceId, invoiceNo, tenant])
            except Exception as e:
                print(f"⚠️ Error checking invoice {invoiceNo} (tenant {tenant}): {e}")

    if( data ):
        save_to_csv("str_dn_validation_v3.csv" , ['invoice_id' , 'invoice_no' , 'tenant'] , data , OUTPUT_DIR)

process_csv()

