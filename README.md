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

### Loading the features with Pyspark

```bash
./pyspark/run_pyspark.sh
```


### Running Locust

```bash
locust -f locustfile.py
```

# Running Pyspark

