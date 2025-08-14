import sys
import os
import csv
import pymysql
import requests
import logging
from datetime import datetime
from collections import defaultdict

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pdi import pdiToTenantMap
from getDBConnection import create_db_connection
from csv_utils import append_to_csv
from getAllWarehouse import getAllWarehouse
from getAllArsenal import getAllArsenal

OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR/CSV_FILES"

def fetchDistinctDebitNoteNumbersWithPdi(tenant, pdis):
    try:
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        placeholders = ','.join(['%s'] * len(pdis))
        query = f"""
            SELECT DISTINCT debit_note_number, partner_detail_id
            FROM purchase_issue
            JOIN purchase_issue_item ON purchase_issue.id = purchase_issue_item.purchase_issue_id
            WHERE debit_note_number IS NOT NULL
            AND invoice_date > '2025-05-28'
            AND pr_type <> 'REGULAR_EASYSOL'
            AND partner_detail_id IN ({placeholders})
        """
        cursor.execute(query, pdis)
        return cursor.fetchall()
        
    except Exception as e:
        print(f"Error fetching debit note numbers for tenant {tenant}: {e}")
        return []
    
def fetchDCForTenant(tenant, listOfDcs):
    try:
        if not listOfDcs:
            return []
        conn = create_db_connection(tenant)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        placeholders = ','.join(['%s'] * len(listOfDcs))
        query = f"SELECT DISTINCT invoice_no FROM inward_invoice WHERE invoice_no IN ({placeholders})"
        cursor.execute(query, listOfDcs)
        return cursor.fetchall()
        
    except Exception as e:
        print(f"Error fetching DC for tenant {tenant}: {e}")
        return []

def fetchDCForAllTenants(warehouse):
    
    for tenant_info in warehouse:
        final_data = []
        
        tenant = tenant_info[0] if isinstance(tenant_info, (list, tuple)) else str(tenant_info)
        print("Processing tenant:", tenant)
        
        pdis = list(pdiToTenantMap.keys())
        purchaseIssueData = fetchDistinctDebitNoteNumbersWithPdi(tenant, pdis)

        print(len(purchaseIssueData))

        pdiToDcMap = defaultdict(list)
        for pi in purchaseIssueData:
            if(pi["debit_note_number"].startswith('PE')): continue
            pdiToDcMap[pi['partner_detail_id']].append(pi['debit_note_number'])

        for pdi, dc_list in pdiToDcMap.items():
            dest_tenant = pdiToTenantMap.get(str(pdi))

            destDCList = fetchDCForTenant(dest_tenant, dc_list)
            destDCNumbers = {row['invoice_no'] for row in destDCList}

            for dc in dc_list:
                if dc not in destDCNumbers:
                    final_data.append([dc, dest_tenant, tenant])
    
        if final_data:
            append_to_csv(
                "dest_invoice_not_created_output_STR.csv",
                ["source_debit_note_number", "dest_tenant", "source_tenant"],
                final_data,
                OUTPUT_DIR
            )

if __name__ == "__main__":
    theas = getAllWarehouse()
    fetchDCForAllTenants(theas)  

