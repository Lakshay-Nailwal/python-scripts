import sys
import os
import csv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from csv_utils import save_to_csv


INPUT_CSV = "/Users/lakshay.nailwal/Desktop/CSV_FILES/dest_invioce_not_created.csv"
OUTPUT_FILE_NAME = "dest_invioce_not_created_output_padded_ucode.csv"

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
    "isUcodeMissingInDestination"
]

already_processed = {}
output_data = []
def process_csv():
    with open(INPUT_CSV, newline='') as infile:    
        reader = csv.DictReader(infile)

        row_count = 1
        for row in reader:
            print(f"Processing row {row_count}")
            row_count += 1
            db_name = row["dest_tenant"]
            ucode = row["source_ucode"].zfill(6)


            print(f"Processing ({db_name}, {ucode})")

            if (db_name, ucode) in already_processed:
                row["isUcodeMissingInDestination"] = already_processed[(db_name, ucode)]
                output_data.append(row)
                continue

            try:
                connection = create_db_connection(db_name)
                cursor = connection.cursor()
                cursor.execute("SELECT 1 FROM inward_invoice_item WHERE code = %s LIMIT 1", (ucode,))
                result = cursor.fetchone()
                row["isUcodeMissingInDestination"] = "No" if result else "Yes"
                already_processed[(db_name, ucode)] = row["isUcodeMissingInDestination"]
            except Exception as e:
                print(f"Error processing ({db_name}, {ucode}): {e}")
                row["isUcodeMissingInDestination"] = "Error"
            finally:
                if connection:
                    connection.close()
            output_data.append(row)

    save_to_csv(OUTPUT_FILE_NAME, csvHeaders, output_data)

if __name__ == "__main__":
    process_csv()
