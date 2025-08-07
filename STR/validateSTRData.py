import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
import csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/CSV_FILES/dest_invioce_not_created.csv"

already_processed = []
successDebitNoteNumbers = []
def process_csv():
    with open(INPUT_CSV, newline='') as infile:    
        reader = csv.DictReader(infile)

        count = 0

        for row in reader:
            
            db_name = row["dest_tenant"]
            if(db_name == "th903"):
                continue
            
            source_debit_note_number = row["source_debit_note_number"]

            if(source_debit_note_number in already_processed):
                continue
            else:
                already_processed.append(source_debit_note_number)

            connection = create_db_connection(db_name)
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM inward_invoice WHERE invoice_no = %s", (source_debit_note_number,))
            result = cursor.fetchall()
            if result:
                count += 1
                successDebitNoteNumbers.append(source_debit_note_number)
                print(f"STR data found for {source_debit_note_number} in {db_name}")
            else:
                print(f"No STR data found for {source_debit_note_number} in {db_name}")

        
    print("Total unique records processed: ", len(already_processed))
    print("Total STR data found: ", count)
    print("Successfully processed debit note numbers: ", successDebitNoteNumbers)
                    
if __name__ == "__main__":
    process_csv()