from utils.BigQueryConnector import connectToBigQuery
import json
import os

from utils.DownloadGCStoLocal import downloadObject

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)

json_file_path = os.path.join(parent_dir, 'config', 'config.json')

with open(json_file_path) as f:
    config = json.load(f)

json_path = config['json_path']
query = config['query']
bucket_name = config['bucket_name']
csv_file = config['csv_file']
connectToBigQuery(json_path, query, bucket_name, csv_file)
file_path = config['bigQuery_output_file_path']
downloadObject(bucket_name, file_path)
