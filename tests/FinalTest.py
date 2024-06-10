import argparse
import logging
import os
import sys

from utils.ConstantsData import region, project_id, bucket_name, new_cluster_name
from utils.SubmitJobToCluster import SubmitJobToCluster
from utils.CreateClusterTest import create_cluster, delete_cluster
from utils.UploadFileToGCS import UploadFileToGCS

fileName = 'CompareCSVAndGenerateOutput.py'
zone = 'us-central1-f'
parser = argparse.ArgumentParser(description="Process some arguments.")
parser.add_argument('--params', nargs='*', help='Argument should be in Key-value pairs', default=[])
logger = logging.getLogger(__name__)
fileHandler = logging.FileHandler('logfile.log')
formatter = logging.Formatter("%(asctime)s :%(levelname)s : %(name)s :%(message)s")
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)  # filehandler object
logger.setLevel(logging.INFO)
args = parser.parse_args()

logger.info(f"Checking number of parameters passed to the script")
if (len(args.params) - 1) / 2 != int(args.params[0].split('=')[1]):
    print("Count value does not match with number of files(source/target files) passed...")
    sys.exit(1)
logger.info(f"Checked : SUCCESS")

current_file = os.path.abspath(__file__)
root_dir = os.path.dirname(current_file)
print(os.path.dirname(root_dir))
root_dir = os.path.dirname(root_dir)
file_path = os.path.join(root_dir, "utils/"f"{fileName}")
destination_blob_name = f'{fileName}'
logger.info(f"Uploading python file i.e. {fileName} on GCS bucket")
UploadFileToGCS(file_path, bucket_name, destination_blob_name)
logger.info(f"{fileName} is uploaded on GCS bucket successfully")
logger.info(f"Creating the new cluster with name : {new_cluster_name}")
status1 = create_cluster(project_id, region, new_cluster_name, zone)
if status1:
    assert status1, "Failed to create the Cluster. Assertion failed."
logger.info(f"{new_cluster_name} has been created and started successfully!")
try:
    logger.info(f"Submitting the job on dataproc {new_cluster_name} cluster")
    job_output = SubmitJobToCluster(project_id, region, new_cluster_name, f'{fileName}', args.params)
    if job_output:
        assert job_output, "Job submission failed. Assertion failed."
    logger.info(f"Job has been Submitted successfully")
finally:
    logger.info(f"Deleting the {new_cluster_name}")
    status2 = delete_cluster(project_id, region, new_cluster_name)
    if status2:
        assert status2, "Failed to delete the Cluster. Assertion failed."
    logger.info(f"{new_cluster_name} has been deleted successfully!")