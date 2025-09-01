import csv
import os
from collections import defaultdict

INPUT_FILE = "inv_purchase_issue_report_20250901_185654.csv"  # replace with your actual file
OUTPUT_DIR = "tenant_sql_updates"
BATCH_SIZE = 4000

os.makedirs(OUTPUT_DIR, exist_ok=True)

def generate_sql_files():
    tenant_records = defaultdict(list)

    # Step 1: Read CSV and group IDs by tenant
    with open(INPUT_FILE, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tenant = row["tenant"]
            pi_id = row["id"]
            invoice_sequence_type = row["invoice_sequence_type"]
            if tenant == "th214" and pi_id == "377910":
                print(f"Debug: seq_type for tenant={tenant}, id={pi_id} → {invoice_sequence_type}")
            tenant_records[tenant].append((pi_id, invoice_sequence_type))

    # Step 2: Write SQL files tenant-wise
    for tenant, records in tenant_records.items():
        file_path = os.path.join(OUTPUT_DIR, f"{tenant}_updates.sql")
        with open(file_path, "w") as f:
            batch = defaultdict(list)

            for pi_id, seq_type in records:
                batch[seq_type].append(pi_id)

                # Flush if batch size reached
                if len(batch[seq_type]) >= BATCH_SIZE:
                    ids_str = ",".join(batch[seq_type])
                    seq_type_safe = seq_type.replace("'", "''")  # escape quotes
                    query = (
                        f"UPDATE {tenant}.purchase_issue "
                        f"SET updated_on = NOW(), invoice_sequence_type = '{seq_type_safe}' "
                        f"WHERE id IN ({ids_str});\n "
                    )
                    f.write(query)
                    batch[seq_type] = []  # reset batch

            # Flush remaining IDs for each seq_type
            for seq_type, ids in batch.items():
                if ids:
                    ids_str = ",".join(ids)
                    seq_type_safe = seq_type.replace("'", "''")
                    query = (
                        f"UPDATE {tenant}.purchase_issue "
                        f"SET updated_on = NOW(), invoice_sequence_type = '{seq_type_safe}' "
                        f"WHERE id IN ({ids_str});\n"
                    )
                    f.write(query)

        print(f"Generated SQL file: {file_path} with {len(records)} updates.")

if __name__ == "__main__":
    generate_sql_files()