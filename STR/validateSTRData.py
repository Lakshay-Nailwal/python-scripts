import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
import csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/CSV_FILES/dest_invioce_not_created.csv"
OUTPUT_CSV = "/Users/lakshay.nailwal/Desktop/CSV_FILES/STR_DATA_VALIDATED.csv"

already_processed = []
def process_csv():
    with open(INPUT_CSV, newline='') as infile:    
        reader = csv.DictReader(infile)

        for row in reader:
            
            db_name = row["dest_tenant"]
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
                print(f"STR data found for {source_debit_note_number} in {db_name}")
            else:
                print(f"No STR data found for {source_debit_note_number} in {db_name}")

            for resultRow in result:
                print(resultRow)
    print("Total unique records processed: ", len(already_processed))
                    
if __name__ == "__main__":
    process_csv()