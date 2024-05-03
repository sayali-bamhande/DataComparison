from google.cloud import dataproc_v1

# Create a client with the desired version
client_options = {"api_endpoint": "us-central1-dataproc.googleapis.com:443"}
client = dataproc_v1.ClusterControllerClient(client_options=client_options)

# Define the project and zone
project_id = 'myprojecthsbc'
region = 'us-central1'
cluster_name = 'cluster-123'
zone = 'us-central1-f'

# Define the cluster configuration
cluster = {
    "project_id": project_id,
    "cluster_name": cluster_name,
    "config": {
        "gce_cluster_config": {
            "zone_uri": f"projects/{project_id}/zones/{zone}",
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "n1-standard-2",
            "disk_config":{
                "boot_disk_size_gb":500
            }
        },
        "worker_config": {
            # "num_instances": 0,
           # "machine_type_uri": "n1-standard-4",
        },
    }
}

# Create the cluster
operation = client.create_cluster(
    request={"project_id": project_id, "region": region, "cluster": cluster}
)

# Wait for the operation to complete
response = operation.result()

# Print cluster details
print("Cluster created successfully:", response.cluster_name)

# Now, let's delete the cluster
cluster_delete_request = {
    "project_id": project_id,
    "region": region,
    "cluster_name": "your-cluster-name",
}

delete_operation = client.delete_cluster(
    request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
)

# Wait for the delete operation to complete
delete_response = delete_operation.result()

# Print confirmation
print("Cluster deleted successfully.")
