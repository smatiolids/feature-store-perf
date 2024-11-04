import os
from dotenv import load_dotenv
load_dotenv(override=True)


import uuid
import random
from datetime import datetime
import time
import csv
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra.cqlengine.query import BatchStatement
from cassandra.query import BatchType
from conn import getCQLSession
from cassandra.cluster import (
    Session
)

# Load environment variables


class AstraWriter:
    session: Session
    csv_writer: csv.writer
    csv_file: object  # file object

    def write_to_csv(self, user_id: uuid.UUID, geo: str):
        """Helper method to write user-geo data to CSV file"""
        self.csv_writer.writerow([str(user_id), geo])
        self.csv_file.flush()  # Ensure it's written to disk immediately

    def __init__(self):
        # Astra connection configuration
        self.session = getCQLSession("astra_db")
        self.session.set_keyspace('demo')

        # Ensure data directory exists
        os.makedirs('data', exist_ok=True)
        
        # Open file for appending and store as class attribute
        self.csv_file = open('data/user_geo_data.csv', 'a', newline='')
        self.csv_writer = csv.writer(self.csv_file)
        
        # Write headers if file is empty
        if os.path.getsize('data/user_geo_data.csv') == 0:
            self.csv_writer.writerow(['user_id', 'geo'])

        # Create table if it doesn't exist
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS features (user_id UUID,
    geo text, 
    merchant_id UUID,
    feature_1 double,
    feature_2 double,
    feature_3 double,
    feature_4 double,
    feature_5 double,
    feature_6 double,
    feature_7 double,
    feature_8 double,
    PRIMARY KEY ((user_id, geo), merchant_id)
);
        """)

    def write_multiple_records(self, num_users, delay_seconds=0):
        """Write multiple records with an optional delay between writes"""
        records_written = 0
        merchants_per_user = 500
        batch_size = 100  # Number of merchants per batch
        query = """
            INSERT INTO features (
                user_id, geo, merchant_id,
                feature_1, feature_2, feature_3, feature_4,
                feature_5, feature_6, feature_7, feature_8
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared_stmt_insert = self.session.prepare(query)

        for _ in range(num_users):
            # Generate user data once
            user_id = uuid.uuid4()
            geo = random.choice(['US', 'UK', 'CA', 'AU', 'DE'])
            
            # Write to CSV file
            self.write_to_csv(user_id, geo)

            # Process merchants in batches
            for batch_start in range(0, merchants_per_user, batch_size):
                try:
                    batch = BatchStatement(batch_type=BatchType.UNLOGGED)

                    # Generate the specified number of merchants for this batch
                    batch_end = min(batch_start + batch_size,
                                    merchants_per_user)
                    for _ in range(batch_start, batch_end):
                        merchant_id = uuid.uuid4()
                        features = [random.uniform(0, 1) for _ in range(8)]

                        batch.add(prepared_stmt_insert, (
                            user_id, geo, merchant_id,
                            features[0], features[1], features[2], features[3],
                            features[4], features[5], features[6], features[7]
                        ))

                    # Execute the batch
                    self.session.execute(batch)
                    records_written += (batch_end - batch_start)
                    print(
                        f"Written batch {batch_start//batch_size + 1}/{merchants_per_user//batch_size} "
                        f"for user {user_id} | Geo: {geo} - Total records: {records_written}/{num_users * merchants_per_user}")

                    if delay_seconds > 0:
                        time.sleep(delay_seconds)

                except Exception as e:
                    print(f"Error writing batch: {e}")

        return records_written

    def close(self):
        self.csv_file.close()
        self.session.shutdown()


if __name__ == "__main__":
    writer = AstraWriter()
    try:
        # Write for 100 users (this will generate 50,000 total records - 100 users * 500 merchants each)
        total_written = writer.write_multiple_records(
            num_users=1000, delay_seconds=0.1)
        print(f"\nSuccessfully wrote {total_written} records")
    finally:
        writer.close()
