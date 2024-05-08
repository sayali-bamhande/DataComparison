from google.cloud import dataproc_v1
#from google.cloud.dataproc_v1 import enums

# Define the project and region
project_id = 'myprojecthsbc'
region = 'us-central1'
cluster_name = 'cluster-1234'

# Create a client
client_options = {"api_endpoint": "us-central1-dataproc.googleapis.com:443"}
client = dataproc_v1.ClusterControllerClient(client_options=client_options)

#client = dataproc_v1.ClusterControllerClient()

# Start the cluster
start_cluster_request = {
    "project_id": project_id,
    "region": region,
    "cluster_name": cluster_name,
}

start_operation = client.start_cluster(request=start_cluster_request)

# Wait for the operation to complete
start_response = start_operation.result()

print("Cluster started successfully:", start_response.cluster_name)

# Stop the cluster
stop_cluster_request = {
    "project_id": project_id,
    "region": region,
    "cluster_name": cluster_name,
}

stop_operation = client.stop_cluster(request=stop_cluster_request)

# Wait for the operation to complete
stop_response = stop_operation.result()

print("Cluster stopped successfully:", stop_response.cluster_name)