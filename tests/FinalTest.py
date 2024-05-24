import argparse
import os
import sys

from utils.CreateClusterTest import create_cluster, delete_cluster
from utils.SubmitJobToCluster import SubmitJobToCluster
from utils.ConstantsData import region, project_id, bucket_name, new_cluster_name

fileName = 'CompareCSVAndGenerateOutput.py'
zone = 'us-central1-f'
parser = argparse.ArgumentParser(description="Process some arguments.")
parser.add_argument('--params', nargs='*', help='Argument should be in Key-value pairs', default=[])

# args = parser.parse_args()
arg = []
for i in range(1, len(sys.argv)):
    print(sys.argv[i], end=" ")
    arg.append(sys.argv[i])

# Parse the key-value pairs and store them in the config module
# arg = []
# for param in args.params:
#     key, value = param.split('=')
#     TestData.params[key] = value
#
# TestData.iteration = TestData.params['count']
print(f"Arguments stored in config: {list}")

current_file = os.path.abspath(__file__)
root_dir = os.path.dirname(current_file)
print(os.path.dirname(root_dir))
root_dir = os.path.dirname(root_dir)
file_path = os.path.join(root_dir, "utils/"f"{fileName}")
destination_blob_name = f'{fileName}'

# UploadFileToGCS(file_path, bucket_name, destination_blob_name)
#create_cluster(project_id, region, new_cluster_name, zone)
SubmitJobToCluster(project_id, region, new_cluster_name, f'{fileName}', f'{arg}')
# delete_cluster(project_id, region, new_cluster_name)
