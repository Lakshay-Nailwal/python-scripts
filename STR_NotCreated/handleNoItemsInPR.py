import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/CSV_FILES/dest_invioce_not_created_output_STR.csv"
OUTPUT_CSV = "movePrToCancelStatus.csv"

def fetchPurchaseIssueItems(purchase_issue_id, tenant):
    connection = create_db_connection(tenant)
    try:
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT * FROM purchase_issue_item WHERE purchase_issue_id = %s", (purchase_issue_id,))
        return cursor.fetchall()
    finally:
        cursor.close()
        connection.close()

def process_csv():
    data = []
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            source_debit_note_number = row["source_debit_note_number"]
            source_tenant = row["source_tenant"]
            dest_tenant = row["dest_tenant"]

            connection = create_db_connection(source_tenant)
            try:
                cursor = connection.cursor(pymysql.cursors.DictCursor)
                cursor.execute("SELECT * FROM purchase_issue WHERE debit_note_number = %s", (source_debit_note_number,))
                purchaseIssues = cursor.fetchall()
            finally:
                cursor.close()
                connection.close()

            for pi in purchaseIssues:
                id = pi["id"]
                items = fetchPurchaseIssueItems(id, source_tenant)
                if len(items) == 0:
                    # Keep the query exactly as you wrote it, fixing only the syntax error:
                    updatePrQuery = f"""
                        UPDATE {source_tenant}.purchase_issue set updated_on = NOW(), status = 'CANCELLED' where id = {id} and debit_note_number = '{source_debit_note_number}' and status = '{pi["status"]};'
                    """
                    data.append([updatePrQuery.strip(), source_tenant, dest_tenant , source_debit_note_number])

    save_to_csv(OUTPUT_CSV, ["query", "source_tenant", "dest_tenant" , "source_debit_note_number"], data)

if __name__ == "__main__":
    process_csv()
