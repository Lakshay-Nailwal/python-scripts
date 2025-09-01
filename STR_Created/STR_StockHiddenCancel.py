import sys
import os
import csv
import pymysql
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv

INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Created/CSV_FILES/STR_StockHiddenCancel_Input.csv"
OUTPUT_DIR = "/Users/lakshay.nailwal/Desktop/updatedScripts/STR_Created/CSV_FILES"

def getInvoiceDetails(db_name, source_debit_note_number):
    connection = create_db_connection(db_name)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM inward_invoice WHERE invoice_no = %s and status = 'StockHidden'",
                (source_debit_note_number,)
            )
            return cursor.fetchall()
    finally:
        connection.close()

def getGatepassDetails(db_name, source_debit_note_number, gatepass_id):
    connection = create_db_connection(db_name)
    try:
        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                "SELECT * FROM gatepass_invoice WHERE gatepass_id = %s and no = %s",
                (gatepass_id, source_debit_note_number)
            )
            return cursor.fetchall()
    finally:
        connection.close()

alreadyProcessedDN = []

def process_csv():
    failedDebitNoteNumbers = []
    cancelledGatepassNumbers = []
    cancelledInvoiceNumbers = []
    deleteDigestData = []
    with open(INPUT_CSV, newline='') as infile:
        reader = csv.DictReader(infile)

        for row in reader:
            db_name = row["dest_tenant"]
            source_debit_note_number = row["source_debit_note_number"]
            source_tenant = row["source_tenant"]

            if source_debit_note_number in alreadyProcessedDN:
                continue
            else:
                alreadyProcessedDN.append(source_debit_note_number)

            print("tenant : ", db_name, "source_debit_note_number : ", source_debit_note_number)

            invoiceDetails = getInvoiceDetails(db_name, source_debit_note_number)
            if len(invoiceDetails) > 0:
                for invoiceDetail in invoiceDetails:
                    if invoiceDetail["status"] == "StockHidden":
                        cancelInvoiceQuery = (
                            f"UPDATE {db_name}.inward_invoice SET status = 'CANCELLED' "
                            f"WHERE id = {invoiceDetail['id']} and invoice_no = '{source_debit_note_number}' "
                            f"and status = 'StockHidden';"
                        )
                        cancelledInvoiceNumbers.append([source_debit_note_number, db_name, row["source_tenant"], cancelInvoiceQuery])

                        deleteDigestQuery = ""
                        CUTOFF_DATE = datetime(2025, 8, 22)
                        print(invoiceDetail["created_on"])
                        print(CUTOFF_DATE)
                        if(invoiceDetail["created_on"] >= CUTOFF_DATE):
                            deleteDigestQuery = f"DELETE FROM mercury.idempotent_digest WHERE metadata = '{invoiceDetail['invoice_no']} + {source_tenant}';"
                            deleteDigestData.append([deleteDigestQuery])

                        gatepassDetails = getGatepassDetails(db_name, source_debit_note_number, invoiceDetail["gatepass_id"])
                        if len(gatepassDetails) > 0:
                            for gatepassDetail in gatepassDetails:
                                cancelGatepassQuery = (
                                    f"UPDATE {db_name}.gatepass_invoice SET status = 'CANCELLED' "
                                    f"WHERE id = {gatepassDetail['id']} and no = '{source_debit_note_number}' "
                                    f"and status = '{gatepassDetail['status']}';"
                                )
                                cancelledGatepassNumbers.append([source_debit_note_number, db_name, row["source_tenant"], cancelGatepassQuery])
            else:
                failedDebitNoteNumbers.append([source_debit_note_number, db_name, row["source_tenant"]])

    # save_to_csv("cancelledInvoiceNumbers_STR.csv",
    #             ["source_debit_note_number", "dest_tenant", "source_tenant", "cancelInvoiceQuery"],
    #             cancelledInvoiceNumbers, OUTPUT_DIR)

    # save_to_csv("cancelledGatepassNumbers_STR.csv",
    #             ["source_debit_note_number", "dest_tenant", "source_tenant", "cancelGatepassQuery"],
    #             cancelledGatepassNumbers, OUTPUT_DIR)
    
    save_to_csv("deleteDigestQuery_STR.csv",
                ["deleteDigestQuery"],
                deleteDigestData, OUTPUT_DIR)

    save_to_csv("failedDebitNoteNumbers_STR.csv",
                ["source_debit_note_number", "dest_tenant", "source_tenant"],
                failedDebitNoteNumbers, OUTPUT_DIR)

    print("total already processed debit note numbers : ", len(alreadyProcessedDN))

process_csv()
