import re
from google.cloud import dataproc_v1 as dataproc
from google.cloud import storage
import subprocess
import sys

from utils.ConstantsData import bucket_name


def SubmitJobToCluster(project_id, region, cluster_name, args):
    # Create the job client.
    main_file_name = 'CompareCSVAndGenerateOutput.py'
    job_client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the job config. 'main_jar_file_uri' can also be a
    # Google Cloud Storage URL.
    print(f"Main file is  : {main_file_name}")
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket_name}/{main_file_name}",
            "args": args
        },

    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )

    print(f"Arguments have been passed to job : {args}")
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

    return True
