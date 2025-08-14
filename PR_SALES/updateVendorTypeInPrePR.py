import sys
import os
import csv
import pymysql

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from getDBConnection import create_db_connection
from csv_utils import save_to_csv
from getAllWarehouse import getAllWarehouse


def query_db(db_name, query, params=None, dict_cursor=True):
    """Run a DB query and return results."""
    db_connection = create_db_connection(db_name)
    cursor_type = pymysql.cursors.DictCursor if dict_cursor else None
    cursor = db_connection.cursor(cursor_type)
    try:
        cursor.execute(query, params or ())
        return cursor.fetchall()
    finally:
        cursor.close()
        db_connection.close()


OUTPUT_CSV = "update_vendor_type_in_pre_pr.csv"


def getPrePurchaseIssueDetails(source_tenant):
    return query_db(
        source_tenant,
        "SELECT id, vendor_type, status, created_on, updated_on "
        "FROM pre_purchase_issue_order "
        "WHERE vendor_type IN ('PRIMARY', 'SECONDARY') "
        "AND status = 'CREATED' AND created_on > '2025-01-01'",
        ()
    )


def getUpdatePrePrQuery():
    warehouses = getAllWarehouse()
    data = []

    print(warehouses)

    for tenant in warehouses:
        print(tenant)
        if tenant == "th438":
            continue

        pre_purchase_issue_details = getPrePurchaseIssueDetails(tenant)

        for issue in pre_purchase_issue_details:
            update_query = (
                f"UPDATE {tenant}.pre_purchase_issue_order "
                f"SET vendor_type = 'REGULAR' "
                f"WHERE id = {issue['id']} "
                f"AND vendor_type = '{issue['vendor_type']}';"
            )
            data.append([
                tenant,
                issue['id'],
                issue['vendor_type'],
                issue['status'],
                issue['created_on'],
                issue['updated_on'],
                update_query
            ])

    return data


if __name__ == "__main__":
    data = getUpdatePrePrQuery()
    csv_header = [
        "tenant", "pre-pr-issue-id", "vendor_type",
        "status", "created_on", "updated_on", "update_query"
    ]
    save_to_csv(OUTPUT_CSV, csv_header, data)
