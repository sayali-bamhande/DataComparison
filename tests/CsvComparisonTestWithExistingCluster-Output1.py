import os

from utils.StartCluster import startTheCluster
from utils.StopCluster import stopTheCluster
from utils.SubmitJobToCluster import SubmitJobToCluster
from utils.UploadFileToGCS import uploadToGCS
from utils.ConstantsData import region, project_id, cluster_name, bucket_name


fileName = 'CsvDataComparisonUsingDataframe.py'
zone = 'us-central1-f'


current_file = os.path.abspath(__file__)
root_dir = os.path.dirname(current_file)
print(os.path.dirname(root_dir))
root_dir = os.path.dirname(root_dir)
file_path = os.path.join(root_dir, "utils/"f"{fileName}")
destination_blob_name = f'{fileName}'
uploadToGCS(file_path, bucket_name, destination_blob_name)
startTheCluster(project_id, region, cluster_name)
SubmitJobToCluster(project_id, region, cluster_name, f'{fileName}')
stopTheCluster(project_id, region, cluster_name)
