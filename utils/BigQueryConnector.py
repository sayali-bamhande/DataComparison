from google.cloud import bigquery
from google.cloud import storage
import os
import csv
import pandas as pd


def connectToBigQuery(json_path, query, bucket_name, csv_file):
    global blob
    # Set the environment variable to point to your service account key file
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
    # Create a BigQuery client
    client = bigquery.Client()
    # Define your SQL query

    # Run the query
    query_job = client.query(query)
    df = query_job.to_dataframe()
    df.to_csv(csv_file, index=True)
    # Initialize GCS client
    storage_client = storage.Client()
    # Replace with your actual GCS bucket name
    blob_name = f"BigQuery/{csv_file}"  # Specify the desired folder and file name in the bucket
    # Create a new blob in the bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    # Upload the data to GCS
    blob.upload_from_filename(csv_file, content_type="text/csv")
    print(f"File uploaded to GCS bucket: gs://{bucket_name}/{blob_name}")
