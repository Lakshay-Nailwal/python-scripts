import pymysql
import logging
import csv
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from pdi import pdiToTenantMap

logging.basicConfig(level=logging.INFO)

DB_HOST = "mercury-prod-replica.crbaj2am3zwb.ap-south-1.rds.amazonaws.com"
DB_USER = "dyno_mayank_mehta_rio_qelae"
DB_PASSWORD = "z23QMqbVQqVST6Ss"
DB_PORT = 3306

PARTNER_TO_TENANT_FILE = "partner_to_tenant.json"
OUTPUT_FILE = f"inv_purchase_issue_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

csv_lock = Lock()


def create_db_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )


def fetch_tenants(connection):
    query = "SELECT tenant FROM mercury.warehouse WHERE is_setup = 1"
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        return [row["tenant"] for row in results]


def fetch_purchase_issues(tenant, partner_to_tenant, writer):
    try:
        conn = create_db_connection()
        query = f"""
            SELECT 
                pi2.id,
                pi2.status,
                pi2.invoice_id,
                pi2.invoice_no,
                pi2.invoice_sequence_type,
                pi2.partner_detail_id,
                pi2.child_tenant_partner_detail_id,
                pi2.pr_type,
                pi2.invoice_sequence_type
            FROM {tenant}.purchase_issue pi2
            WHERE pi2.status NOT IN ('cancelled', 'DELETED')
              AND (pi2.debit_note_number is NOT NULL OR pi2.debit_note_number != '')
              AND pi2.invoice_date > '2025-08-27 00:00:00'
        """
        with conn.cursor() as cursor:
            cursor.execute(query)
            records = cursor.fetchall()

        row_count = 0
        with csv_lock:
            for record in records:
                partner_id = str(record.get("partner_detail_id"))
                tenantCode = partner_to_tenant.get(partner_id, "")
                invoice_sequence_type = (
                    "DEBIT_NOTE_NUMBER" if tenantCode else "DELIVERY_CHALLAN_NORMAL"
                )

                if record.get('invoice_sequence_type') == invoice_sequence_type:
                    continue

                writer.writerow([
                    tenant,
                    record.get("id"),
                    record.get("status"),
                    record.get("invoice_id"),
                    record.get("invoice_no"),
                    partner_id,
                    record.get("child_tenant_partner_detail_id"),
                    tenantCode,
                    invoice_sequence_type
                ])
                row_count += 1

        conn.close()
        return row_count

    except Exception:
        logging.exception(f"Error processing tenant {tenant}")
        return 0


def load_partner_to_tenant_mapping():
    return pdiToTenantMap


if __name__ == "__main__":
    try:
        db_connection = create_db_connection()
        tenants = fetch_tenants(db_connection)
        db_connection.close()

        logging.info(f"Fetched {len(tenants)} tenants to process.")
        partner_to_tenant = load_partner_to_tenant_mapping()

        with open(OUTPUT_FILE, mode="w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "tenant", "id", "status", "invoice_id", "invoice_no",
                "partner_detail_id", "child_tenant_partner_detail_id",
                "tenantCode", "invoice_sequence_type"
            ])

            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_tenant = {
                    executor.submit(fetch_purchase_issues, tenant, partner_to_tenant, writer): tenant
                    for tenant in tenants
                }

                for future in as_completed(future_to_tenant):
                    tenant = future_to_tenant[future]
                    try:
                        count = future.result()
                        logging.info(f"Tenant {tenant} processed with {count} records.")
                    except Exception:
                        logging.exception(f"Error in tenant {tenant}")

        logging.info(f"Processing completed. Output saved to {OUTPUT_FILE}")

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Flushing messages...")
    except Exception:
        logging.exception("Unexpected error occurred during execution.")
