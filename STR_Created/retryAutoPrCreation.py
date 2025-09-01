import sys
import os
import csv
import pymysql
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from pdi import pdiToTenantMap

tenants = ['th400' , 'th213' , 'th437' , 'th223']

CURRENT_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CSV_FILES")
csv_lock = Lock()

SQL_QUERY = """
    SELECT pi.id as purchase_issue_id, pi.partner_detail_id as ogPdi,  pi.invoice_id as ogInvoiceId , pi.invoice_no as ogInvoiceNo , pi.source_invoice_id, pi.reference_debit_note_number , pi.status , ii.purchase_type , ii.partner_detail_id 
    FROM purchase_issue pi
    JOIN inward_invoice ii ON ii.id = pi.source_invoice_id
    WHERE source_type = 'MANUAL'
    AND pi.created_on >= '2025-08-29'
    AND pi.source_invoice_id IS NOT NULL
    AND pi.reference_debit_note_number IS NOT NULL
    AND pi.reference_debit_note_number != ''
    AND pi.status NOT IN ('cancelled', 'DELETED')
    AND ii.purchase_type IN ('StockTransferReturn', 'ICSReturn')
    AND (pi.debit_note_number IS NULL OR pi.debit_note_number = '')
"""

def fetchPurchaseIssuesForTenant(tenant):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute(SQL_QUERY)
        return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching purchase issues for tenant {tenant}: {e}")
        return []

pdis = list(pdiToTenantMap.keys())
def processPurchaseIssueWithReferenceDebitNoteNumber(reference_debit_note_number, source_tenant):
    try:
        conn = create_db_connection(source_tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        cursor.execute("SELECT distinct child_tenant_partner_detail_id FROM purchase_issue WHERE debit_note_number = %s", (reference_debit_note_number,))
        result = cursor.fetchall()
        child_tenant_partner_detail_ids = [row["child_tenant_partner_detail_id"] for row in result]
        for child_tenant_partner_detail_id in child_tenant_partner_detail_ids:
            if child_tenant_partner_detail_id not in pdis:
                return child_tenant_partner_detail_id
        return None
    except Exception as e:
        print(f"Error fetching purchase issues for tenant {source_tenant}: {e}")
        return []

def process_tenant(tenant):
    try:
        data = []
        print(f"Processing tenant: {tenant}")
        fetchPurchaseIssues = fetchPurchaseIssuesForTenant(tenant)
        seenReferenceDebitNoteNumbers = set()
        refDebitNoteNumberToPdiMap = {}
        for purchaseIssue in fetchPurchaseIssues:
            reference_debit_note_number = purchaseIssue["reference_debit_note_number"]
            if reference_debit_note_number in seenReferenceDebitNoteNumbers:
                continue
            seenReferenceDebitNoteNumbers.add(reference_debit_note_number)

            source_tenant = pdiToTenantMap[str(purchaseIssue["partner_detail_id"])]
            
            correctPdi = processPurchaseIssueWithReferenceDebitNoteNumber(reference_debit_note_number, source_tenant)
            if correctPdi is None:
                continue

            refDebitNoteNumberToPdiMap[reference_debit_note_number] = correctPdi


        refDebitNoteNumberToUpdateMap = {}
        for purchaseIssue in fetchPurchaseIssues:
            if refDebitNoteNumberToUpdateMap.get(purchaseIssue["reference_debit_note_number"]) is None:
                refDebitNoteNumberToUpdateMap[purchaseIssue["reference_debit_note_number"]] = False
            reference_debit_note_number = purchaseIssue["reference_debit_note_number"]
            ogPdi = purchaseIssue["ogPdi"]
            if ogPdi == refDebitNoteNumberToPdiMap.get(ogPdi):
                continue
            refDebitNoteNumberToUpdateMap[reference_debit_note_number] = True

        for purchaseIssue in fetchPurchaseIssues:
            if refDebitNoteNumberToUpdateMap.get(purchaseIssue["reference_debit_note_number"]) == True:
                purchase_issue_id = int(purchaseIssue['purchase_issue_id'])
                reference_debit_note_number = (purchaseIssue["reference_debit_note_number"])
                status = (purchaseIssue["status"])
                correct_pdi = refDebitNoteNumberToPdiMap.get(purchaseIssue["reference_debit_note_number"])

                cancelQuery = (
                    f"UPDATE {tenant}.purchase_issue "
                    f"SET updated_on = NOW(), status = 'CANCELLED' "
                    f"WHERE id = {purchase_issue_id} "
                    f"AND reference_debit_note_number = '{reference_debit_note_number}' "
                    f"AND status = '{status}';"
                )
                data.append([
                    cancelQuery, tenant, reference_debit_note_number,
                    purchaseIssue["ogPdi"], purchaseIssue["ogInvoiceId"],
                    purchaseIssue["ogInvoiceNo"], correct_pdi
                ])

        if data:
            with csv_lock:
                append_to_csv("retryAutoPrCreation.csv", ["query", "tenant", "reference_debit_note_number", "ogPdi", "ogInvoiceId", "ogInvoiceNo", "correctPdi"], data, CURRENT_DIRECTORY)
        else:
            print("No data to save")
    except Exception as e:
        print(f"Error processing tenant {tenant}: {e}")
        return
    

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_tenant, tenant) for tenant in tenants]
        for future in as_completed(futures):
            future.result()

    