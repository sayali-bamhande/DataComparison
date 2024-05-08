import json
import os

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)

json_file_path = os.path.join(parent_dir, 'config', 'config.json')

with open(json_file_path) as f:
    config = json.load(f)

json_path = config['json_path']
query = config['query']
bucket_name = config['bucket_name']
project_id = config['project_id']
cluster_name = config['cluster_name']
new_cluster_name = config['custer_to_create']
region = config['region']
zone = config['zone']

csv_file = config['csv_file']
file_path = config['bigQuery_output_file_path']

file1_path = config['DB2_file']
file2_path = config['BigQuery_file']

