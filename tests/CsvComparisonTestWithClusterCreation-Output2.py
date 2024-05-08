import os

from utils.CreateClusterTest import create_cluster, delete_cluster
from utils.SubmitJobToCluster import SubmitJobToCluster
from utils.UploadFileToGCS import upload_to_gcs
from utils.ConstantsData import region, project_id, bucket_name, new_cluster_name

fileName = 'CompareCSVAndGenerateOutput.py'
zone = 'us-central1-f'

current_file = os.path.abspath(__file__)
root_dir = os.path.dirname(current_file)
print(os.path.dirname(root_dir))
root_dir = os.path.dirname(root_dir)
file_path = os.path.join(root_dir, "utils\\"f"{fileName}")
destination_blob_name = f'{fileName}'

upload_to_gcs(file_path, bucket_name, destination_blob_name)
create_cluster(project_id, region, new_cluster_name, zone)
SubmitJobToCluster(project_id,region,new_cluster_name,{fileName})
delete_cluster(project_id, region, new_cluster_name)




