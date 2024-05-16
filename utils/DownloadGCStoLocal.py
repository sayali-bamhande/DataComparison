import os

from google.cloud import storage

# Initialize a client
client = storage.Client()


def downloadObject(bucket_name,file_path):

    # local_file = "pythonProject/output/GCS_data.csv"
    # Read the file
    blob = client.bucket(bucket_name).blob(file_path)
    content = blob.download_as_text()
    current_dir = os.path.dirname(__file__)
    parent_dir = os.path.dirname(current_dir)
    json_file_path = os.path.join(parent_dir, 'output', 'GCS_data.csv')
    with open(json_file_path, 'wb') as f:
        f.write(content.encode('utf-8'))
    print(f"Content of {file_path}:\n{content}")


# Specify the GCS bucket and file path
bucket_name = "mybucket_hsbc"
file_path = "BigQuery/BigQuery.csv"
downloadObject(bucket_name,'Output/Output_20240515_135615.csv')
