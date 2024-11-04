from locust import HttpUser, task, between, events
import random
import csv
import os
from dotenv import load_dotenv
from conn import getCQLSession
from cassandra.query import PreparedStatement, ConsistencyLevel
import uuid
import time

# Load environment variables
load_dotenv(override=True)

# Global session and prepared statement for all users
cassandra_session = None
prepared_select = None
user_geo_data = None

class UserGeoData:
    def __init__(self, filepath):
        self.user_geos = []
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.user_geos.append({
                    'user_id': row['user_id'],
                    'geo': row['geo']
                })
    
    def get_random_user_geo(self):
        return random.choice(self.user_geos)

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize Cassandra session and load data before test starts"""
    global cassandra_session, prepared_select, user_geo_data
    
    # Initialize Cassandra connection
    cassandra_session = getCQLSession("astra_db")
    cassandra_session.set_keyspace('demo')
    
    # Prepare the select statement with LOCAL_ONE consistency
    prepared_select = cassandra_session.prepare("""
        SELECT user_id,geo,merchant_id,feature_1,feature_2,feature_3 FROM features 
        WHERE user_id = ? AND geo = ?
    """)
    prepared_select.consistency_level = ConsistencyLevel.LOCAL_ONE
    
    # Load user-geo data once
    user_geo_data = UserGeoData('../data/user_geo_data.csv')
    print("Test initialized")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Clean up Cassandra session after test ends"""
    global cassandra_session
    if cassandra_session:
        cassandra_session.shutdown()
    print("Test stopped")

class FeatureAPIUser(HttpUser):
    wait_time = between(0.1, 1.0)  # Wait between 0.1-1 seconds between tasks
    
    @task  # Now this is the only task, no need for weight
    def get_user_features_driver(self):
        """Get features using Cassandra driver directly"""
        user_geo = user_geo_data.get_random_user_geo()
        start_time = time.time()
        
        try:
            # Execute query and measure time
            result = cassandra_session.execute(
                prepared_select, 
                (uuid.UUID(user_geo['user_id']), user_geo['geo'])
            )
            total_time = (time.time() - start_time) * 1000
            
            # Convert result to list to get length
            rows = list(result)
            
            # Report success to Locust
            self.environment.events.request.fire(
                request_type="SELECT",
                name="cassandra_driver_features",
                response_time=total_time,
                response_length=len(rows),
                exception=None,
                context={},
            )
                
        except Exception as e:
            # Report failure to Locust
            self.environment.events.request.fire(
                request_type="SELECT",
                name="cassandra_driver_features",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
                context={},
            )