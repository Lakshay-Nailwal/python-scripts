import sys
import os
import csv
import pymysql.cursors
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import save_to_csv


INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES/purchase_issue_items_with_ucode_missing_in_dest_for_non_dc_v6.csv"
OUTPUT_FILE_NAME = "delete_purchase_issue_item_output_and_move_Pre_purchase_issue_order_v8.csv"
OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES"
output_data = []

def handle_pre_purchase_issue_order_and_purchase_issue_item():
    print("handle_pre_purchase_issue_order_and_purchase_issue_item")
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            purchaseIssueItemId = row["purchase_issue_item_id"]
            source_tenant = row["tenant"]

            connection = create_db_connection(source_tenant)
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""
                SELECT pii.*, pi.pre_purchase_issue_id as ppio_in_PR, pi.tray_id 
                FROM purchase_issue_item pii 
                JOIN purchase_issue pi ON pi.id = pii.purchase_issue_id 
                WHERE pii.id = %s
            """, (purchaseIssueItemId))
            result = cursor.fetchall()
            connection.close()

            newTray = row["Tray"]
            if newTray == "No": continue

            if newTray == "":
                newTray = result[0]["tray_id"]

            for r in result:
                prePrId = r.get("pre_purchase_issue_id") or r.get("ppio_in_PR")

                if prePrId:
                    update_pre_purchase_query = (
                        f"UPDATE {source_tenant}.pre_purchase_issue_order SET updated_on = NOW(), status = 'CREATED', tray_id = '{newTray}' WHERE id = {prePrId};"
                    )
                    delete_purchase_issue_item_query = (
                        f"DELETE FROM {source_tenant}.purchase_issue_item WHERE id = {r['id']};"
                    )

                    enriched_row = {
                        **r,  # Include all columns from pii.* and ppio_in_PR
                        "source_tenant": source_tenant,
                        "updated_Pre_PR_tray": newTray,
                        "old_tray": r["tray_id"],
                        "update_pre_purchase_query": update_pre_purchase_query,
                        "delete_purchase_issue_item_query": delete_purchase_issue_item_query
                    }

                    output_data.append(enriched_row)
                else:
                    print(f"⚠️ No pre_purchase_issue_order found for {purchaseIssueItemId} in {source_tenant}")
                    continue

    if output_data:
        csv_headers = list(output_data[0].keys())
        save_to_csv(OUTPUT_FILE_NAME, csv_headers, output_data, OUTPUT_DIR)
    else:
        print("⚠️ No data written to output CSV")
                


INPUT_CSV_3 = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES/delete_purchase_issue_item_output_and_move_Pre_purchase_issue_order_v8.csv"

def handle_purchase_issue_item_invoices():
    print("handle_purchase_issue_item_invoices")
    output_data = []

    with open(INPUT_CSV_3, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:

            db_name = row["source_tenant"]
            purchase_issue_item_id = row["id"]

            connection = create_db_connection(db_name)
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            query = """
                SELECT * FROM purchase_issue_invoice WHERE purchase_issue_item_id = %s
            """
            params = (purchase_issue_item_id)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            connection.close()

            if not results:
                print(f"❌ No purchase_issue_invoice found for {purchase_issue_item_id} in DB: {db_name}")
                continue

            for invoice_row in results:
                delete_query = (
                    f"DELETE FROM {db_name}.purchase_issue_invoice "
                    f"WHERE purchase_issue_item_id = {purchase_issue_item_id};"
                )

                # Add db name and delete query to the row
                invoice_row["db_name"] = db_name
                invoice_row["delete_purchase_issue_item_invoice_query"] = delete_query

                output_data.append(invoice_row)

    if output_data:
        csv_headers = list(output_data[0].keys())
        save_to_csv("delete_purchase_issue_item_invoice_output_v8.csv", csv_headers, output_data, OUTPUT_DIR)
    else:
        print("⚠️ No data to write.")

if __name__ == "__main__":
    handle_pre_purchase_issue_order_and_purchase_issue_item()
    handle_purchase_issue_item_invoices()