import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import sys

def sumbitjob1(project_id, region, cluster_name):
    # Create the job client.
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.

    # job = {
    #     "placement": {"cluster_name": cluster_name},
    #     "pyspark_job": {
    #         "main_python_file_uri":f"gs://dumybucket123/Code/distinct.py"
    #     },
    # }

    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://dumybucket123/Code/CsvDataComparisonUsingDataframe.py" ##utils/CsvDataComparisonUsingDataframe.py_old
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # # Fetch and print the job ID
    # job_id = response.reference.job_id
    #
    # print(f"Submitted job with ID:{job_id}")

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

project_id = 'hsbcnikhil'
region = 'us-central1'
cluster_name = 'your-cluster-name'
sumbitjob1(project_id,region,cluster_name)
