import argparse
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

args = parser.parse_args()

if (len(args.params)-1)/2 != int(args.params[0].split('=')[1]):
    print("Count value does not match with number of files(source/target files) passed...")
    sys.exit(1)

current_file = os.path.abspath(__file__)
root_dir = os.path.dirname(current_file)
print(os.path.dirname(root_dir))
root_dir = os.path.dirname(root_dir)
file_path = os.path.join(root_dir, "utils/"f"{fileName}")
destination_blob_name = f'{fileName}'

UploadFileToGCS(file_path, bucket_name, destination_blob_name)
create_cluster(project_id, region, new_cluster_name, zone)
SubmitJobToCluster(project_id, region, new_cluster_name, f'{fileName}', args.params)
delete_cluster(project_id, region, new_cluster_name)
