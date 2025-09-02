import sys
import os
import csv
import requests
import time
import pymysql
import json

# Add parent directory to import custom modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from getDBConnection import create_db_connection
from token_switcher import get_token_for_tenant
from pdi import pdiToTenantMap
from kafka import KafkaProducer

# Input CSV file path
INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Created/CSV_FILES/retryAutoPrCreation.csv"


from typing import List, Optional
from dataclasses import dataclass, asdict

@dataclass
class PurchaseIssueItem:
    code: Optional[str]
    name: Optional[str]
    batch: Optional[str]
    returnReason: Optional[str]
    returnQuantity: Optional[int]
    barcode: Optional[str] = None
    vendorId: Optional[int] = None
    ucode: Optional[str] = None
    bin: Optional[str] = None

@dataclass
class PurchaseIssue:
    id: Optional[int] = None
    invoiceNo: Optional[str] = None
    invoiceId: Optional[int] = None
    sourceInvoiceId: Optional[int] = None
    defects: List[PurchaseIssueItem] = None
    autoSTReturnInwardEnabled: Optional[bool] = False
    referenceDebitNoteNumber: Optional[str] = None

@dataclass
class AutoPurchaseIssueCreationDTO:
    invoiceId: int
    purchaseIssue: PurchaseIssue
    user: str
    tenant: str


def fetchInwardInvoiceForTenant(tenant, debitNoteNumber):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT id , invoice_no , partner_detail_id FROM inward_invoice WHERE invoice_no = %s and purchase_type in ('StockTransferReturn', 'ICSReturn') and status = 'live' and created_on >= '2025-08-29'", (debitNoteNumber,))
    inwardInvoice = cursor.fetchone()
    cursor.close()
    conn.close()
    return inwardInvoice

def fetchAutoStReturnInwardEnabled(currentTenantPdi , inwardInvoicePdi):
    conn = create_db_connection("mercury")
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT auto_st_return_inward_enabled FROM vendor_hop_tenant_mapping WHERE source_pdi = %s and destination_pdi = %s", (currentTenantPdi, inwardInvoicePdi))
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    if result is None or result["auto_st_return_inward_enabled"] == False:
        return False
    return True
    
def fetchPurchaseIssueItemsForTenant(tenant, debitNoteNumber):
    conn = create_db_connection(tenant)
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT pii.ucode , pii.batch , pii.return_quantity , pii.name , pii.return_reason FROM purchase_issue_item pii JOIN purchase_issue pi ON pi.id = pii.purchase_issue_id WHERE pi.debit_note_number = %s", (debitNoteNumber,))
    purchaseIssueItems = cursor.fetchall()
    cursor.close()
    conn.close()
    return purchaseIssueItems


def prepareDataForKafka(inwardInvoice, correctPdi, prItems , autoStReturnInwardEnabled , tenant):
    purchaseIssueItems = []
    for item in prItems:
        purchaseIssueItems.append(PurchaseIssueItem(
            code=item["ucode"],
            name=item["name"],
            batch=item["batch"],
            returnReason=item["return_reason"],
            returnQuantity=item["return_quantity"],
            ucode=item["ucode"],
            barcode=None,
            vendorId=correctPdi,
            bin="EMPTY_BIN"
        ))
        
    purchaseIssue = PurchaseIssue(
        id=inwardInvoice["id"],
        invoiceNo=inwardInvoice["invoice_no"],
        defects=purchaseIssueItems,
        autoSTReturnInwardEnabled=autoStReturnInwardEnabled,
    )
    autoPurchaseIssueCreationDTO = AutoPurchaseIssueCreationDTO(
        invoiceId=inwardInvoice["id"],
        purchaseIssue=purchaseIssue,
        user="SYSTEM",
        tenant= tenant
    )
    return asdict(autoPurchaseIssueCreationDTO)



def publish_to_kafka(data):
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=[
                'kafka01.neo.mercuryonline.co:9092',
                'kafka02.neo.mercuryonline.co:9092',
                'kafka03.neo.mercuryonline.co:9092'
            ],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        kafka_producer.send('mco_auto_create_purchase_issue', value=data)
        kafka_producer.flush()
        kafka_producer.close()
    except Exception as e:
        print(f"Error publishing data to Kafka: {e}")

def process_csv():
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)
        seenDebitNoteNumbers = set()
        for row in reader:
            debitNoteNumber = row["reference_debit_note_number"]
            if debitNoteNumber in seenDebitNoteNumbers:
                continue
            seenDebitNoteNumbers.add(debitNoteNumber)

            tenant = row["tenant"]
            inwardInvoice = fetchInwardInvoiceForTenant(tenant, debitNoteNumber)

            key = [k for k, v in pdiToTenantMap.items() if v == str(tenant)]
            currentTenantPdi = key[0]

            source_tenant = pdiToTenantMap[str(inwardInvoice["partner_detail_id"])]

            autoStReturnInwardEnabled = fetchAutoStReturnInwardEnabled(currentTenantPdi , inwardInvoice["partner_detail_id"])

            prItems = fetchPurchaseIssueItemsForTenant(source_tenant, inwardInvoice["invoice_no"])

            correctPdi = row["correctPdi"]

            data = prepareDataForKafka(inwardInvoice, correctPdi, prItems , autoStReturnInwardEnabled , tenant)
            publish_to_kafka(data)

if __name__ == "__main__":
    process_csv()