import argparse
import os
import sys
import logging
from utils.ConstantsData import region, project_id, cluster_name, bucket_name, new_cluster_name, zone
from utils.CreateClusterTest import delete_cluster, create_cluster
from utils.StartCluster import startTheCluster
from utils.StopCluster import stopTheCluster
from utils.UploadFileToGCS import UploadFileToGCS
from utils.SubmitJobToCluster import SubmitJobToCluster





def main():
    global fileName
    fileName = 'CompareCSVAndGenerateOutput.py'
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument('--params', nargs='*', help='Argument should be in Key-value pairs', default=[])
    args = parser.parse_args()
    test_submit_job_to_cluster(fileName, args)

def test_submit_job_to_cluster(fileName, args):
    # logger file provide logs


    #global fileName
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
    UploadFileToGCS(file_path, bucket_name, destination_blob_name)
    logger.info(f"{fileName} is uploaded on GCS bucket successfully (code required to generate output in format 1)")

    logger.info(f"Starting the {new_cluster_name}")
    status1 = create_cluster(project_id, region, new_cluster_name, zone)
    if status1:
        assert status1, "Failed to create the Cluster. Assertion failed."

    logger.info(f"{new_cluster_name} is created and started successfully!")
    try:
        logger.info(f"Submitting the Job")
        job_output = SubmitJobToCluster(project_id, region, new_cluster_name, f'{fileName}', args.params)

        if job_output:
            assert job_output, "Job submission failed. Assertion failed."
            # delete_cluster(project_id, region, new_cluster_name)
            # logger.info(f"Cluster has been deleted successfully")

        logger.info(f"Job has been Submitted successfully")

        # If the assertion passes, the program continues
        logger.info("File has been generated successfully!")
    # except:
    #     logger.info("Something went wrong in submitting job")
    finally:
        logger.info(f"Deleting the {new_cluster_name}")
        status2 = delete_cluster(project_id, region, new_cluster_name)
        if status2:
            assert status2, "Failed to delete the Cluster. Assertion failed."

        logger.info(f"{new_cluster_name} is deleted successfully!")


if __name__ == '__main__':
    #zone = 'us-central1-f'
   main()