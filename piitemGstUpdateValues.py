import csv
import requests
import time
import logging
import pymysql
import sys

ERP_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJhcHAiOiJuZWJ1bGEiLCJhdWQiOiJtZXJjdXJ5IiwidWlkIjoiZGM3MWMwMTYtNjk3NC00OTg5LWFjZjctNDQ4NTkxOTIyZGNmIiwiaXNzIjoiUGhhcm1FYXN5LmluIiwibmFtZSI6Ik1heWFuayBNZWh0YSIsInN0b3JlIjoiYTgwNzE2NzAtODUwNS00ZjhiLWJjN2UtMDQ3ZjZkYWI1M2YyIiwic2NvcGVzIjpbIndoLWFkbWluIiwid2gtc3VwZXItYWRtaW4iXSwiZXhwIjoxNzU2NzUzMTU5LCJ1c2VyIjoibWF5YW5rLm1laHRhQHBoYXJtZWFzeS5pbiIsInRlbmFudCI6InRoNDI1In0.OZ0OaXBAxBRYYGhmBYLmkjiWw6MboxHtT6t1CKHjCCcenSxsF_cFPcQbAPD7lK03cQ06xVHeeesoKgqPVfha-A'  # 🔑 Base ERP token

DB_HOST = "mercury-prod-replica.crbaj2am3zwb.ap-south-1.rds.amazonaws.com"
DB_USER = "dyno_mayank_mehta_rio_qelae"
DB_PASSWORD = "z23QMqbVQqVST6Ss"
DB_PORT = 3306

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

def create_db_connection():
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )
        logging.info("Database connection established.")
        return conn
    except pymysql.MySQLError as e:
        logging.error(f"Database connection failed: {e}")
        sys.exit(1)

def fetch_warehouse_id(connection, tenant):
    query = "SELECT id FROM mercury.warehouse WHERE is_setup = 1 AND tenant = %s LIMIT 1"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, (tenant,))
            result = cursor.fetchone()
            return result["id"] if result else None
    except Exception as e:
        logging.error(f"Failed to fetch warehouse ID for tenant {tenant}: {e}")
        return None

def switch(warehouse_id, tenant):
    url = f'https://wms.mercuryonline.co/api/user/auth/switch/warehouse/{warehouse_id}'
    headers = {
        'Authorization': f'{ERP_TOKEN}',
        'Content-Type': 'application/json'
    }
    try:
        logging.info(f"Switching to tenant {tenant} with URL: {url}")
        response = requests.post(url, headers=headers, timeout=10)
        if response.status_code == 200:
            logging.info(f"Token obtained for tenant: {tenant}")
            return response.json()['token']
        else:
            logging.error(f"Error switching to tenant {tenant}: {response.status_code} - {response.text}")
            return None
    except requests.RequestException as e:
        logging.error(f"Exception while switching tenant {tenant}: {e}")
        return None

def process_csv(file_path):
    tenant_tokens = {}   # ✅ Cache tenant tokens
    processed_set = set()
    success_rows = []
    failed_rows = []

    try:
        db_connection = create_db_connection()
    except Exception as e:
        logging.error(f"Cannot create DB connection: {e}")
        return

    rows = []
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                tenant = row.get('tenant')
                invoice_id = row.get('invoice_id')
                invoice_seq_type = row.get('invoice_sequence_type')
                purchase_issue_id = row.get('id')

                if not tenant or not purchase_issue_id:
                    row["error_reason"] = "Missing tenant or id"
                    failed_rows.append(row)
                    continue

                # ✅ Process only if DEBIT_NOTE_NUMBER and invoice_id is NULL/blank
                if invoice_seq_type == "DEBIT_NOTE_NUMBER" and (not invoice_id or invoice_id.strip() == ""):
                    key = (tenant, purchase_issue_id)
                    rows.append((key, row))
    except FileNotFoundError:
        logging.error(f"CSV file not found: {file_path}")
        return
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return

    for (key, row) in rows:
        tenant, purchase_issue_id = key

        if key in processed_set:
            continue

        # ✅ Generate tenant token only once
        if tenant not in tenant_tokens:
            warehouse_id = fetch_warehouse_id(db_connection, tenant)
            if not warehouse_id:
                row["error_reason"] = "No warehouse ID found"
                failed_rows.append(row)
                continue

            token = switch(warehouse_id, tenant)
            if not token:
                row["error_reason"] = "Failed to switch warehouse / get token"
                failed_rows.append(row)
                continue

            tenant_tokens[tenant] = token
        else:
            token = tenant_tokens[tenant]

        url = 'https://wms.mercuryonline.co/api/inward/purchase_returns/updatePurchaseIssueItem'
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json'
        }
        payload = {
            'purchaseIssueId': int(purchase_issue_id),
            'disableGstApplicable': True
        }

        logging.info(f"Calling API for tenant {url}: {payload}")

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                logging.info(f"[{tenant}] purchaseIssueId {purchase_issue_id} | success")
                success_rows.append({"tenant": tenant, "purchaseIssueId": purchase_issue_id})
            else:
                error_msg = f"API error {response.status_code}: {response.text}"
                logging.error(f"[{tenant}] Failed API call {purchase_issue_id}: {error_msg}")
                row["error_reason"] = error_msg
                failed_rows.append(row)
        except requests.RequestException as e:
            logging.error(f"Error hitting API for tenant {tenant}: {e}")
            row["error_reason"] = f"RequestException: {e}"
            failed_rows.append(row)

        processed_set.add(key)
        time.sleep(0.1)

    # ✅ Write success rows
    if success_rows:
        success_file = 'successful_purchase_issues.csv'
        with open(success_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["tenant", "purchaseIssueId"])
            writer.writeheader()
            writer.writerows(success_rows)
        logging.info(f"Successful purchaseIssueIds written to {success_file}")

    # ✅ Write failed rows with error_reason
    if failed_rows:
        failed_file = 'failed_rows_disable_gst.csv'
        fieldnames = list(failed_rows[0].keys())
        with open(failed_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(failed_rows)
        logging.info(f"Failed rows written to {failed_file}")

if __name__ == "__main__":
    try:
        process_csv("inv_purchase_issue_report_20250826_180833.csv")
    except Exception as e:
        logging.error(f"Unexpected error during script execution: {e}")