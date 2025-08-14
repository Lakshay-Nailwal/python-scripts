import sys
import os
import csv
import pymysql.cursors
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import save_to_csv


INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/CSV_FILES/failed_debit_notes.csv"
OUTPUT_FILE_NAME = "delete_purchase_issue_item_output_and_move_Pre_purchase_issue_order_v2.csv"

output_data = []

def handle_pre_purchase_issue_order_and_purchase_issue_item():
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            debitNote = row["debitNote"]
            source_tenant = row["source_tenant"]
            ucode = row["ucode"].zfill(6)
            batch = row["batch"]

            connection = create_db_connection(source_tenant)
            cursor = connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute("""
                SELECT pii.*, pi.pre_purchase_issue_id as ppio_in_PR 
                FROM purchase_issue_item pii 
                JOIN purchase_issue pi ON pi.id = pii.purchase_issue_id 
                WHERE pi.debit_note_number = %s AND pii.ucode = %s AND pii.batch = %s
            """, (debitNote, ucode, batch))
            result = cursor.fetchall()
            connection.close()

            if len(result) == 0:
                print(f"❌ No purchase_issue_item found for {debitNote} in {source_tenant} with ucode {ucode} and batch {batch}")
                continue

            for r in result:
                prePrId = r.get("pre_purchase_issue_id") or r.get("ppio_in_PR")

                if prePrId:
                    update_pre_purchase_query = (
                        f"UPDATE {source_tenant}.pre_purchase_issue_order SET updated_on = NOW(), status = 'CREATED' "
                        f"WHERE id = {prePrId}"
                    )
                    delete_purchase_issue_item_query = (
                        f"DELETE FROM {source_tenant}.purchase_issue_item WHERE id = {r['id']}"
                    )

                    enriched_row = {
                        **r,  # Include all columns from pii.* and ppio_in_PR
                        "debitNote": debitNote,
                        "source_tenant": source_tenant,
                        "ucode": ucode,
                        "batch": batch,
                        "message": "Purchase issue item deleted and pre purchase issue order moved to CREATED status",
                        "update_pre_purchase_query": update_pre_purchase_query,
                        "delete_purchase_issue_item_query": delete_purchase_issue_item_query
                    }

                    output_data.append(enriched_row)
                else:
                    print(f"⚠️ No pre_purchase_issue_order found for {debitNote} in {source_tenant} with ucode {ucode} and batch {batch}")
                    continue

    if output_data:
        csv_headers = list(output_data[0].keys())
        save_to_csv(OUTPUT_FILE_NAME, csv_headers, output_data)
    else:
        print("⚠️ No data written to output CSV")
                    

INPUT_CSV_2 = "/Users/lakshay.nailwal/Desktop/updatedScripts/CSV_FILES/ucode_missing_dest_invoice_not_created_output.csv"

def handle_purchase_return_inventory():
    print("handle_purchase_return_inventory")
    
    from collections import defaultdict

    return_quantities = defaultdict(int)  # now int instead of float
    
    with open(INPUT_CSV_2, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:

            db_name = row["source_tenant"]
            assigned_bin = row["assigned_bin"]
            return_qty = int(row["return_quantity"])

            key = (db_name, assigned_bin)
            return_quantities[key] += return_qty

    output_data = []
    csvHeaders = [
        "db_name",
        "assigned_bin",
        "original_quantity",
        "total_return_quantity",
        "new_quantity",
        "update_purchase_return_inventory_query"
    ]

    for (db_name, assigned_bin), total_return_qty in return_quantities.items():
        connection = create_db_connection(db_name)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = "SELECT quantity FROM purchase_return_inventory WHERE bin = %s"
        params = (assigned_bin)
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        connection.close()

        if not result:
            print(f"❌ No inventory found for bin: {assigned_bin} in DB: {db_name}")
            continue

        original_qty = result["quantity"]
        new_qty = original_qty + total_return_qty

        update_query = (
            f"UPDATE {db_name}.purchase_return_inventory SET updated_on = NOW(), quantity = {new_qty} "
            f"WHERE bin = '{assigned_bin}' AND quantity = {original_qty};"
        )

        output_data.append({
            "db_name": db_name,
            "assigned_bin": assigned_bin,
            "original_quantity": original_qty,
            "total_return_quantity": total_return_qty,
            "new_quantity": new_qty,
            "update_purchase_return_inventory_query": update_query
        })

    save_to_csv("update_return_inventory_queries.csv", csvHeaders, output_data)


def handle_dc_update_on_our_side():
    pass

INPUT_CSV_2 = "/Users/lakshay.nailwal/Desktop/CSV_FILES/updatedScripts/delete_purchase_issue_item_output_and_move_Pre_purchase_issue_order_v2.csv"

def handle_purchase_issue_item_invoices():
    print("handle_purchase_issue_item_invoices")
    output_data = []

    with open(INPUT_CSV_2, newline='') as infile:
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
        save_to_csv("delete_purchase_issue_item_invoice_output.csv", csv_headers, output_data)
    else:
        print("⚠️ No data to write.")


if __name__ == "__main__":
    handle_pre_purchase_issue_order_and_purchase_issue_item()
    handle_purchase_issue_item_invoices()
    handle_purchase_return_inventory()
    # handle_dc_update_on_our_side()

