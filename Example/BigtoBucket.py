from google.cloud import bigquery
from google.cloud import storage
import os
import csv
import pandas as pd

# Set the path to your service account key file
json_path = "C://Users//Sayali.Bamhande//Documents//myprojecthsbc-3a9b0e7c4b4f.json"
# Set the environment variable to point to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

# Create a BigQuery client
client = bigquery.Client()

# Define your SQL query
query = """SELECT * FROM `bigquery-public-data.austin_311.311_service_requests` 
            where status in ('Closed','Open')
            order by status_change_date
            LIMIT 1500;
         """

# Run the query
query_job = client.query(query)

df = query_job.to_dataframe()
csv_file = "BigQuery.csv"
df.to_csv(csv_file, index=True)

# Initialize GCS client
storage_client = storage.Client()
bucket_name = "mybucket_hsbc"  # Replace with your actual GCS bucket name
blob_name = "BigQuery/BigQuery.csv"  # Specify the desired folder and file name in the bucket

# Create a new blob in the bucket
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)

# Write the query results directly to the GCS blob
# csv_data = "\n".join([f"{row.url},{row.view_count}" for row in query_job])

# Add headers to the CSV data
# csv_data_with_headers = "URL,View Count\n" + csv_data

# Upload the data to GCS
blob.upload_from_filename(csv_file, content_type="text/csv")

print(f"File uploaded to GCS bucket: gs://{bucket_name}/{blob_name}")
