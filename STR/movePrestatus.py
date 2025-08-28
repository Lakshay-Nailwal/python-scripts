import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import append_to_csv


INPUT_CSV = "/Users/lakshay.nailwal/Desktop/updatedScripts/CSV_FILES/demo.csv"


def getPrIssue(tenant, id):
    connection = create_db_connection(tenant)
    cursor = connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(f"SELECT * FROM {tenant}.purchase_issue WHERE pre_purchase_issue_id = {id}")
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return result


def main():
    total_count = 0
    ids = {}
    with open(INPUT_CSV, 'r') as file:
        reader = csv.reader(file)
        count = 0
        for row in reader:
            if count == 0:
                count += 1
                continue
            count += 1



            id = row[1]
            tenant = row[0]
            pr = getPrIssue(tenant, id)

            if len(pr) == 0:
                if tenant not in ids:
                    ids[tenant] = []
                ids[tenant].append(id)
                print(f"No pre purchase issue order found for {id} in {tenant}")            
                # updateQuery = f"UPDATE {tenant}.pre_purchase_issue_order SET updated_on = created_on , status = 'COMPLETED' WHERE id = {id} AND status = 'CREATED'"

                # append_to_csv("move_pre_purchase_issue_order_status_v5.csv", ["tenant", "updateQuery"], [[tenant, updateQuery]] , None, False)

    print(ids)


main()






