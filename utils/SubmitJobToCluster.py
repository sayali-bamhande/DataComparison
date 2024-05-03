import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import subprocess
import sys


def SubmitJobToCluster(project_id, region, cluster_name, main_file):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    print("Main file is  : "+main_file)
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://mybucket_hsbc/{main_file}"
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output gets saved to the Google Cloud Storage bucket
    # allocated to the job. Use a regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_bytes()
        .decode("utf-8")
    )

    print(f"Job finished successfully: {output}")

# project_id = 'hsbcnikhil'
# region = 'us-central1'
# cluster_name = 'your-cluster-name'
# SubmitJobToCluster(project_id,region,cluster_name)
# bucket_name = 'mybucket_hsbc'
# project_id = 'myprojecthsbc'
# region = 'us-central1'
# cluster_name = 'cluster-11e2'
# SubmitJobToCluster(project_id,region,cluster_name)


###########################################################
# import pytest
# from google.cloud import storage
# from google.cloud import dataproc_v1 as dataproc
# import re
#
# # Fixture for setting up the Google Cloud Storage client
# @pytest.fixture(scope="module")
# def storage_client():
#     return storage.Client()
#
# # Fixture for setting up the Dataproc job client
# @pytest.fixture(scope="module")
# def job_client(region):
#     return dataproc.JobControllerClient(
#         client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
#     )
#
# # Test function to upload the .py file and submit the job
# def SubmitJobToCluster(storage_client, job_client, bucket_name, file_path, project_id, region, cluster_name):
#     # Upload the .py file to the bucket
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(file_path)
#     blob.upload_from_filename(file_path)
#
#     assert blob.exists(), "File upload failed."
#
#     # Create the job config
#     job = {
#         "placement": {"cluster_name": cluster_name},
#         "pyspark_job": {
#             "main_python_file_uri": f"gs://{bucket_name}/{file_path}"
#         },
#     }
#
#     # Submit the job to the cluster
#     operation = job_client.submit_job_as_operation(
#         request={"project_id": project_id, "region": region, "job": job}
#     )
#     response = operation.result()
#
#     # Verify the job submission
#     assert response.status.state == dataproc.types.JobStatus.State.DONE, "Job submission failed."
#
#     # Dataproc job output gets saved to the Google Cloud Storage bucket
#     # allocated to the job. Use a regex to obtain the bucket and blob info.
#     matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)
#     assert matches, "Failed to parse the output resource URI."
#
#     output_blob = storage_client.bucket(matches.group(1)).blob(f"{matches.group(2)}.000000000")
#     assert output_blob.exists(), "Job output file not found."
#
#     output = output_blob.download_as_bytes().decode("utf-8")
#     print(f"Job finished successfully: {output}")
#
# # Example usage of the test function
# def main():
#     project_id = 'hsbcnikhil'
#     region = 'us-central1'
#     cluster_name = 'your-cluster-name'
#     bucket_name = 'dumybucket123'
#     file_path = 'C://Users//Nikhil.Dhulekar//PycharmProjects//pythonProject//utils//CsvDataComparisonUsingDataframe.py_old'  # Local path to the Python script
#
#     SubmitJobToCluster(storage_client, job_client,bucket_name, file_path, project_id, region, cluster_name)
#
# if __name__ == "__main__":
#     main()
