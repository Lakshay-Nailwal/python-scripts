import sys
import os
import csv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/CSV_FILES/dest_invoice_not_created_output_STR.csv"
OUTPUT_CSV = "dest_invioce_not_created_output_STR_v45.csv"

already_processed = []
successDebitNoteNumbers = []
failedDebitNoteNumbers = []
failedCSVHeader = ["source_debit_note_number", "source_tenant", "dest_tenant"]

def process_csv():
    with open(INPUT_CSV, newline='') as infile:    
        reader = csv.DictReader(infile)
        count = 0

        for row in reader:
            db_name = row["dest_tenant"]
            if db_name == "th903":
                continue

            source_debit_note_number = row["source_debit_note_number"]

            if(source_debit_note_number.startswith("PE")):continue

            if source_debit_note_number in already_processed:
                continue
            already_processed.append(source_debit_note_number)

            connection = create_db_connection(db_name)
            cursor = connection.cursor()
            cursor.execute(
                "SELECT * FROM inward_invoice WHERE invoice_no = %s", 
                (source_debit_note_number,)
            )
            result = cursor.fetchall()

            if result:
                count += 1
                successDebitNoteNumbers.append(source_debit_note_number)
            else:
                failedDebitNoteNumbers.append(
                    (source_debit_note_number, row["source_tenant"], db_name)
                )
                print(f"No STR data found for {source_debit_note_number} in {db_name}")

        print("Total unique records processed:", len(already_processed))
        print("Total STR data found:", count)
        print("Successfully processed debit note numbers:", successDebitNoteNumbers)
        
        save_to_csv(OUTPUT_CSV, failedCSVHeader, failedDebitNoteNumbers)

if __name__ == "__main__":
    process_csv()
