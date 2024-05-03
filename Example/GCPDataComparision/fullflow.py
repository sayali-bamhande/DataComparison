# Import necessary libraries
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import pandas as pd
from pyspark.sql import SparkSession

# Set your GCP project ID, region, and cluster name
project_id = "hsbcnikhil"
region = "us-central1"
cluster_name = "your-cluster-name"

# Initialize a Dataproc client
client = dataproc.ClusterControllerClient(client_options={"api_endpoint": f"{region}-dataproc.googleapis.com"})

# Define the cluster configuration (assuming you already have an existing cluster)
cluster_config = {
    "project_id": project_id,
    "cluster_name": cluster_name,
}

# Read data from Google Cloud Storage
storage_client = storage.Client()
bucket_name = "dumybucket123"
file1_path = "Bigquery/BigqueryData.csv"
file2_path = "DB2/DB2data.csv"

bucket = storage_client.bucket(bucket_name)
blob1 = bucket.blob(file1_path)
blob2 = bucket.blob(file2_path)



# Create a SparkSession
spark = SparkSession.builder.appName("Data Discrepancy").getOrCreate()

# Read the two tables into DataFrames
table1 = spark.read.format("csv").option("header", "true").load("table1.csv")
table2 = spark.read.format("csv").option("header", "true").load("table2.csv")

# Compare the tables and find the rows that are in table1 but not in table2
# discrepancy1 = table1.except(table2)

# Show the rows in the discrepancy DataFrame
# discrepancy1.show()

# Compare the tables and find the rows that are in table2 but not in table1
# discrepancy2 = table2.except(table1)

# Show the rows in the discrepancy DataFrame
# discrepancy2.show()

#
# # Compare DataFrames row by row
# diff_df = df1.compare(df2)
#
# # Print the differences
# print("Differences between file1 and file2:")
print(discrepancy2)

# Optionally, save the differences to a new CSV file in a different folder in Google Cloud Storage
output_folder = "Output"
output_blob = bucket.blob(f"{output_folder}/differences.csv")
output_blob.upload_from_string(discrepancy2.to_csv(index=False))
print(f"Differences saved to {output_blob.name}")
