import pandas as pd
import gcsfs

from Example.sumbitjob1 import Object

Object.sumbitjob1()

# Set your GCP credentials (either via environment variables or directly)
gcp_project = 'hsbcnikhil'
bucket_name = 'dumybucket123'
file_path = 'Bigquery/BigqueryData.csv'

# Read the CSV file into a Pandas DataFrame
df = pd.read_csv(f'gs://{bucket_name}/{file_path}')
