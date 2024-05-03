from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BigQuery with PySpark") \
    .config("spark.jars", "--jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Set up GCP credentials if not already set
# spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "path_to_service_account_key.json")

# Define BigQuery parameters
project_id = "your_project_id"
dataset_id = "your_dataset_id"
table_id = "your_table_id"

# Read data from BigQuery
df = spark.read.format("bigquery") \
    .option("table", f"{project_id}.{dataset_id}.{table_id}") \
    .load()

# Now you can work with df, which is a DataFrame containing the data from BigQuery
df.show()