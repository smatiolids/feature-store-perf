# Feature Store - Astra DB Testing with PySpark & Locust


## Setup 

### Virtual Environment
```bash
python -m venv venv
```
### Activate Virtual Environment

```bash
source venv/bin/activate
```

### Install Requirements
```bash
pip install -r requirements.txt
```

# Loading the features with Python

```bash
python load/load_features.py
```




# Running Pyspark

## Loading the features with Pyspark

```bash
./pyspark/run_pyspark.sh
```

## Detailed command

```bash
$SPARK_HOME/bin/spark-submit \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
--files <secure_connect_bundle file path>.zip  \
--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
load.py 

```


# Running Locust

```bash
locust -f locustfile.py
```
