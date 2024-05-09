import os
import sys
import pytest
import logging
# from utils.CsvDataComparisionDataframe1 import output_path
from utils.ConstantsData import region, project_id, cluster_name, bucket_name
from utils.StartCluster import startTheCluster
from utils.StopCluster import stopTheCluster

# sys.path.append("C:/Users/Sayali.Bamhande/PycharmProjects/DataComparison/utils")
# project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# print("*************", project_dir)
zone = 'us-central1-f'

#sys.path.insert(0, project_dir)
from utils.UploadFileToGCS import upload_to_gcs
from utils.SubmitJobToCluster import SubmitJobToCluster

fileName = 'CompareCSVAndGenerateOutput.py'


@pytest.fixture
def setup():
    global fileName
    current_file = os.path.abspath(__file__)
    root_dir = os.path.dirname(current_file)
    print(os.path.dirname(root_dir))
    root_dir = os.path.dirname(root_dir)
    file_path = os.path.join(root_dir, "utils/"f"{fileName}")
    destination_blob_name = f'{fileName}'
    upload_to_gcs(file_path, bucket_name, destination_blob_name)


@pytest.mark.html_report(output_dir='./reports', report_name='CsvComparisionReport')
def test_submit_job_to_cluster():
    # logger file provide logs
    logger = logging.getLogger(__name__)
    fileHandler = logging.FileHandler('logfile.log')
    formatter = logging.Formatter("%(asctime)s :%(levelname)s : %(name)s :%(message)s")
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)  # filehandler object
    logger.setLevel(logging.INFO)

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
