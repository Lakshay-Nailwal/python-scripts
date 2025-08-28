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
from csv_utils import save_to_csv

# Input CSV file path
INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_DN/CSV_FILES/str_dn_validation_v2.csv"
OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_DN/CSV_FILES"
# Set to track already processed debit note numbers
already_processed = set()
failed_cases = []

def process_csv():
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            tenant = row["tenant"]

            invoiceId = row["invoice_id"]

            invoiceNo = row["invoice_no"]

            already_processed.add(invoiceNo)

            try:
                token = get_token_for_tenant(tenant)
            except Exception as token_error:
                print(f"[TOKEN ERROR] Failed to get token for tenant {tenant}")
                continue

            url = f'https://wms.mercuryonline.co/api/inward/invoices/{invoiceId}/creditNote/retry?tenant={tenant}'
            headers = {
                'Authorization': token,
                'Content-Type': 'application/json'
            }


            # Step 3: Make the API call with raw list of IDs
            try:
                response = requests.post(url, headers=headers, timeout=120)
                if response.status_code == 200:
                    print("Processed --> InvoiceNo : {invoiceNo} , tenant : {tenant}")
                else:
                    failed_cases.append([invoiceId, invoiceNo, response.text ,  tenant])
            except requests.RequestException as e:
                print(f"[EXCEPTION] API request error for tenant {tenant}, InvoiceNo: {invoiceNo}")
                print(e)
                failed_cases.append([invoiceId, invoiceNo, e ,  tenant])

            # Step 4: Wait 1 second to avoid overloading API
            time.sleep(1)

# Run the script
if __name__ == "__main__":
    process_csv()
    print("unique invoice attemp", len(already_processed))
    print("failed_cases", failed_cases)
    save_to_csv("failed_cases_for_STR__DN_retry.csv" , ["invoice_id" , "invoice_no" , "message" , "tenant"] , failed_cases , OUTPUT_DIR)

