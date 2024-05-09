import os
import pytest
import logging

from utils.StartCluster import startTheCluster
from utils.StopCluster import stopTheCluster
from utils.SubmitJobToCluster import SubmitJobToCluster
from utils.UploadFileToGCS import upload_to_gcs
from utils.ConstantsData import region, project_id, cluster_name, bucket_name

fileName = 'CompareCSVAndGenerateOutput.py'
zone = 'us-central1-f'


@pytest.fixture
def setup():
    current_file = os.path.abspath(__file__)
    root_dir = os.path.dirname(current_file)
    print(os.path.dirname(root_dir))
    root_dir = os.path.dirname(root_dir)
    global file_path
    file_path = os.path.join(root_dir, "utils/"f"{fileName}")
    global destination_blob_name
    destination_blob_name = f'{fileName}'

# @pytest.fixture
# def uploadFile():
#     upload_to_gcs(file_path, bucket_name, destination_blob_name)
#
# @pytest.fixture
# def startCluster():
#     startTheCluster(project_id, region, cluster_name)
#
# @pytest.mark.html_report(output_dir='./reports', report_name='CsvComparisionReport')
# def submitJob():
#     SubmitJobToCluster(project_id, region, cluster_name, f'{fileName}')
#
# @pytest.fixture
# def stopCluster():
#     stopTheCluster(project_id, region, cluster_name)
#

# logger.info("File generated successfully!")
