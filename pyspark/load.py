from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, array, lit
from pyspark.sql.types import DoubleType, StringType, ArrayType, StructType, StructField
import uuid
import random
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

def create_spark_session():
    """Create and configure Spark session with Cassandra connector"""
    return (SparkSession.builder
            .appName("Feature Loader")
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0")
            .config("spark.cassandra.connection.config.cloud.path", os.getenv('ASTRA_DB_SECURE_BUNDLE_FILENAME'))
            .config("spark.cassandra.auth.username", os.getenv('ASTRA_DB_CLIENT_ID'))
            .config("spark.cassandra.auth.password", os.getenv('ASTRA_DB_CLIENT_SECRET'))
            .getOrCreate())

def generate_features():
    """Generate random feature values"""
    return [random.uniform(0, 1) for _ in range(8)]

def generate_merchant_id():
    """Generate UUID for merchant"""
    return str(uuid.uuid4())

# Register UDFs
generate_features_udf = udf(generate_features, ArrayType(DoubleType()))
generate_merchant_id_udf = udf(generate_merchant_id, StringType())

def create_user_geo_data(spark, num_users):
    """Create initial user-geo dataframe"""
    # Generate user IDs and geos
    user_data = [(str(uuid.uuid4()), random.choice(['US', 'UK', 'CA', 'AU', 'DE'])) 
                 for _ in range(num_users)]
    
    # Create dataframe
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("geo", StringType(), False)
    ])
    
    df = spark.createDataFrame(user_data, schema)
    
    # Save to CSV for later use by locust
    df.coalesce(1).write.mode("overwrite").csv("data/user_geo_data_pyspark", header=True)
    
    return df

def expand_to_merchants(user_geo_df, merchants_per_user):
    """Expand user-geo data to include merchants and features"""
    # Create array of merchant counts
    merchant_counts = array(*[lit(i) for i in range(merchants_per_user)])
    
    # Explode the merchant counts to create merchant rows
    return (user_geo_df
            .withColumn("merchant_count", explode(merchant_counts))
            .withColumn("merchant_id", generate_merchant_id_udf())
            .withColumn("features", generate_features_udf()))

def write_to_cassandra(df):
    """Write the dataframe to Cassandra"""
    # Expand features array into individual columns
    for i in range(8):
        df = df.withColumn(f"feature_{i+1}", col("features")[i])
    
    # Drop intermediate columns and write to Cassandra
    (df.drop("merchant_count", "features")
       .write
       .format("org.apache.spark.sql.cassandra")
       .mode("append")
       .option("spark.cassandra.output.batch.size.rows","50")
       .options(table="features", keyspace="demo")
       .save())

def main():
    # Configuration
    num_users = 1000
    merchants_per_user = 500
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Create user-geo data
        print("Generating user-geo data...")
        user_geo_df = create_user_geo_data(spark, num_users)
        
        # Expand to include merchants and features
        print("Expanding data with merchants and features...")
        expanded_df = expand_to_merchants(user_geo_df, merchants_per_user)
        
        # Write to Cassandra
        print("Writing to Cassandra...")
        write_to_cassandra(expanded_df)
        
        print(f"Successfully loaded {num_users * merchants_per_user} records")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
