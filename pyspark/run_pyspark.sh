$SPARK_HOME/bin/spark-submit \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
--files /Users/samuel.matioli/work/secure-connect-astrabench_2.zip  \
--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
load.py 

