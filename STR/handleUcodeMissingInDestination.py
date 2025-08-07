import sys
import os
import csv
import pymysql.cursors
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import save_to_csv


INPUT_CSV = "/Users/lakshay.nailwal/Desktop/CSV_FILES/dest_invioce_not_created_output.csv"
OUTPUT_FILE_NAME = "ucode_missing_dest_invoice_not_created_output.csv"

# Include both old + new headers
csvHeaders = [
    "source_tenant",
    "source_debit_note_number",
    "internal_vendor_id",
    "external_vendor_id",
    "source_ucode",
    "source_batch",
    "source_qty",
    "source_DN_amt",
    "source_purchase_issue_invoice_date",
    "source_created_on",
    "source_updated_on",
    "dest_tenant",
    "isUcodeMissingInDestination",
    
    # New fields from query
    "purchase_issue_item_id",
    "purchase_issue_id",
    "invoice_sequence_type",
    "ucode",
    "batch",
    "pre_purchase_issue_id",
    "pre_purchase_status",
    "tray_id",
    "assigned_bin",
    "return_quantity"

    # Generated queries
    "update_pre_purchase_query",
    "delete_purchase_issue_item_query"
]

output_data = []

def get_ucode_mapping():
    ucode_map = {}

    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            key = (row["dest_tenant"], row["source_debit_note_number"])
            ucode = row["source_ucode"]

            if row["isUcodeMissingInDestination"] == "No":
                continue

            if key not in ucode_map:
                ucode_map[key] = []

            ucode_map[key].append((ucode, row))  # also store the original row for reuse

    return ucode_map

def handle_pre_purchase_issue_order_and_purchase_issue_item():
    print("handle_pre_purchase_issue_order_and_purchase_issue_item")
    ucode_map = get_ucode_mapping()

    print("ucode_map", ucode_map)

    for key, ucode_row_pairs in ucode_map.items():
        db_name, source_debit_note_number = key
        ucodes = [u for u, _ in ucode_row_pairs]

        connection = create_db_connection(db_name)
        cursor = connection.cursor(pymysql.cursors.DictCursor)  # get rows as dicts

        query = """
            SELECT pii.id AS purchase_issue_item_id, pi.id AS purchase_issue_id, pi.invoice_sequence_type,
                   pii.ucode, pii.batch, pii.pre_purchase_issue_id, ppio.status AS pre_purchase_status,
                   pi.tray_id, pii.assigned_bin, pii.return_quantity
            FROM purchase_issue pi
            JOIN purchase_issue_item pii ON pii.purchase_issue_id = pi.id
            JOIN pre_purchase_issue_order ppio ON ppio.id = pii.pre_purchase_issue_id
            WHERE pii.ucode IN %s AND pi.debit_note_number = %s
        """
        params = (tuple(ucodes), source_debit_note_number)
        print(f"🔍 Executing SQL Query:")
        print(f"Query: {query}")
        print(f"Parameters: {params}")
        
        cursor.execute(query, params)

        result = cursor.fetchall()
        print("result", result)
        connection.close()

        print("result", result)

        for row in result:
            update_pre_purchase_query = (
                f"UPDATE {db_name}.pre_purchase_issue_order SET updated_on = NOW(), status = 'CREATED' "
                f"WHERE id = {row['pre_purchase_issue_id']} AND status = '{row['pre_purchase_status']}'"
            )
            delete_purchase_issue_item_query = (
                f"DELETE FROM {db_name}.purchase_issue_item WHERE id = {row['purchase_issue_item_id']} "
                f"AND purchase_issue_id = {row['purchase_issue_id']} AND assigned_bin = '{row['assigned_bin']}' "
                f"AND ucode = '{row['ucode']}' AND batch = '{row['batch']}'"
            )

            # Find original row by matching ucode
            original_row = next(orig for u, orig in ucode_row_pairs if u == row["ucode"])

            # Prepare combined output row
            output_row = {
                **original_row,
                **row,
                "update_pre_purchase_query": update_pre_purchase_query,
                "delete_purchase_issue_item_query": delete_purchase_issue_item_query
            }

            output_data.append(output_row)

            print("output_row", output_row)

    save_to_csv(OUTPUT_FILE_NAME, csvHeaders, output_data)

INPUT_CSV_2 = "/Users/lakshay.nailwal/Desktop/CSV_FILES/ucode_missing_dest_invoice_not_created_output.csv"

def handle_purchase_return_inventory():
    print("handle_purchase_return_inventory")
    
    from collections import defaultdict

    return_quantities = defaultdict(int)  # now int instead of float
    
    with open(INPUT_CSV_2, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row["isUcodeMissingInDestination"] == "No":
                continue

            db_name = row["dest_tenant"]
            assigned_bin = row["assigned_bin"]
            return_qty = int(row["source_qty"])

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
        params = (assigned_bin,)
        print(f"🔍 Executing SQL Query:")
        print(f"Query: {query}")
        print(f"Parameters: {params}")
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        connection.close()

        if not result:
            print(f"❌ No inventory found for bin: {assigned_bin} in DB: {db_name}")
            continue

        original_qty = result["quantity"]
        new_qty = original_qty - total_return_qty

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


def handle_purchase_issue_item_invoices():
    print("handle_purchase_issue_item_invoices")
    INPUT_CSV_2 = "/Users/lakshay.nailwal/Desktop/CSV_FILES/ucode_missing_dest_invoice_not_created_output.csv"
    output_data = []

    with open(INPUT_CSV_2, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            if row["isUcodeMissingInDestination"] == "No":
                continue

            db_name = row["dest_tenant"]
            purchase_issue_item_id = row["purchase_issue_item_id"]

            connection = create_db_connection(db_name)
            cursor = connection.cursor(pymysql.cursors.DictCursor)

            query = """
                SELECT * FROM purchase_issue_invoice WHERE purchase_issue_item_id = %s
            """
            params = (purchase_issue_item_id,)
            print(f"🔍 Executing SQL Query:")
            print(f"Query: {query}")
            print(f"Parameters: {params}")
            
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
    handle_dc_update_on_our_side()

