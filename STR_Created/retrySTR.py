import sys
import os
import csv
import requests
import json
import time

# Add parent directory to import custom modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from token_switcher import get_token_for_tenant

# Input CSV file path
INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Created/CSV_FILES/cancelledInvoiceNumbers_STR.csv"

# Set to track already processed debit note numbers
already_processed = set()
failed_cases = []
purchase_issue_ids = []

def process_csv():
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:

            sourceDebitNoteNumber = row["source_debit_note_number"]
            if sourceDebitNoteNumber in already_processed:
                continue

            already_processed.add(sourceDebitNoteNumber)
            sourceTenant = row["source_tenant"]

            # Step 1: Get purchase_issue IDs from source tenant DB
            try:
                connection = create_db_connection(sourceTenant)
                cursor = connection.cursor()
                cursor.execute("""
                    SELECT pi.id FROM purchase_issue pi WHERE pi.debit_note_number = %s
                """, (sourceDebitNoteNumber,))
                result = cursor.fetchall()
            except Exception as db_error:
                print(f"[DB ERROR] Failed DB query for tenant {sourceTenant}, DN: {sourceDebitNoteNumber}")
                print(db_error)
                continue
            finally:
                connection.close()

            # Extract IDs from DB result
            ids = [row[0] for row in result]
            if not ids:
                print(f"[INFO] No purchase_issue found for DN: {sourceDebitNoteNumber} in tenant {sourceTenant}")
                continue

            purchase_issue_ids.extend(ids)
            # Step 2: Prepare token and API request
            try:
                token = get_token_for_tenant(sourceTenant)
            except Exception as token_error:
                print(f"[TOKEN ERROR] Failed to get token for tenant {sourceTenant}")
                print(token_error)
                continue

            url = f'https://wms.mercuryonline.co/api/inward/purchase_returns/retry?tenant={sourceTenant}'
            headers = {
                'Authorization': token,
                'Content-Type': 'application/json'
            }

            # # Step 3: Make the API call with raw list of IDs
            try:
                response = requests.post(url, headers=headers, json=ids, timeout=120)
                if response.status_code == 200:
                    print(f"[SUCCESS] Retry triggered for DN: {sourceDebitNoteNumber} in tenant {sourceTenant}")
                else:
                    print(f"[ERROR] API call failed for tenant {sourceTenant}, DN: {sourceDebitNoteNumber}")
                    print(f"Status: {response.status_code}, Response: {response.text}")
                    failed_cases.append({"debitNote": sourceDebitNoteNumber, "tenant": sourceTenant, "message": response.text})
            except requests.RequestException as e:
                print(f"[EXCEPTION] API request error for tenant {sourceTenant}, DN: {sourceDebitNoteNumber}")
                print(e)
                failed_cases.append({"debitNote": sourceDebitNoteNumber, "tenant": sourceTenant, "message": e , "dest_tenant": de})

            # Step 4: Wait 1 second to avoid overloading API
            time.sleep(1)

# Run the script
if __name__ == "__main__":
    process_csv()
    print("unique dc attemp", len(already_processed))
    print("failed_cases", failed_cases)
    print("purchase_issue_ids", len(purchase_issue_ids))

