import os
import sys
import pytest
import logging
from utils.ConstantsData import region, project_id, cluster_name, bucket_name
from utils.StartCluster import startTheCluster
from utils.StopCluster import stopTheCluster
from utils.UploadFileToGCS import uploadToGCS
from utils.SubmitJobToCluster import SubmitJobToCluster

zone = 'us-central1-f'
fileName = 'CompareCSVAndGenerateOutput.py'


@pytest.mark.html_report(output_dir='./reports', report_name='CsvComparisionReport')
def test_submit_job_to_cluster():
    # logger file provide logs
    global fileName
    current_file = os.path.abspath(__file__)
    root_dir = os.path.dirname(current_file)
    print(os.path.dirname(root_dir))
    root_dir = os.path.dirname(root_dir)

    logger = logging.getLogger(__name__)
    fileHandler = logging.FileHandler('logfile.log')
    formatter = logging.Formatter("%(asctime)s :%(levelname)s : %(name)s :%(message)s")
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)  # filehandler object
    logger.setLevel(logging.INFO)

    file_path = os.path.join(root_dir, "utils/"f"{fileName}")

    logger.info(f"Going to upload {fileName} on GCS bucket")
    destination_blob_name = f'{fileName}'
    uploadToGCS(file_path, bucket_name, destination_blob_name)
    logger.info(f"{fileName} is uploaded on GCS bucket successfully (code required to generate output in format 1)")

    logger.info(f"Starting the {cluster_name}")
    status1 = startTheCluster(project_id, region, cluster_name)
    if status1:
        assert status1, "Failed to start the Cluster. Assertion failed."

    logger.info(f"{cluster_name} is started successfully!")

    logger.info(f"Submitting the Job")
    job_output = SubmitJobToCluster(project_id, region, cluster_name, f'{fileName}')

    if job_output:
        assert job_output, "Job submission failed. Assertion failed."

    logger.info(f"Job Submitted successfully")

    # If the assertion passes, the program continues
    logger.info("File generated successfully!")

    logger.info(f"Stopping the {cluster_name}")
    status2 = stopTheCluster(project_id, region, cluster_name)
    if status2:
        assert status2, "Failed to stop the Cluster. Assertion failed."

    logger.info(f"{cluster_name} is stopped successfully!")
