import csv
import time
from google.cloud import spanner

# Set variables
PROJECT_ID = "demo-project"
INSTANCE_ID = "demo-spanner"
DATABASE_ID = "demo-spanner-db"
TABLE_NAME = "account_activity_raw"
CSV_FILE = "activity_data.csv"

# Initialize Spanner client
spanner_client = spanner.Client(project=PROJECT_ID)
instance = spanner_client.instance(INSTANCE_ID)
database = instance.database(DATABASE_ID)

# Function to load CSV data into Spanner
def load_data():
    with open(CSV_FILE, "r") as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip the header row

        rows = []
        for row in reader:
            rows.append(tuple(row))

            # Batch insert every 500 rows
            if len(rows) >= 500:
                insert_rows(rows)
                rows = []

        # Insert any remaining rows
        if rows:
            insert_rows(rows)

def insert_rows(rows):
    try:
        with database.batch() as batch:
            batch.insert(
                table=TABLE_NAME,
                columns=(
                    "transaction_id", "account_id", "timestamp",
                    "location", "device_type", "ip_address",
                    "transaction_amount", "transaction_type",
                    "successful_login", "unusual_activity"
                ),
                values=rows
            )
        print(f"Inserted {len(rows)} rows successfully")
    except Exception as e:
        print(f"Error inserting rows: {e}")

if __name__ == "__main__":
    start_time = time.time()
    load_data()
    print(f"Data load complete in {time.time() - start_time:.2f} seconds")
