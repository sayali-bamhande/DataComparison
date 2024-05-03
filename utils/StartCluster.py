from google.cloud import dataproc_v1


def startTheCluster(project_id, region, cluster_name):
    client_options = {"api_endpoint": "us-central1-dataproc.googleapis.com:443"}
    client = dataproc_v1.ClusterControllerClient(client_options=client_options)
    # Start the cluster
    start_cluster_request = {
        "project_id": project_id,
        "region": region,
        "cluster_name": cluster_name
    }
    start_operation = client.start_cluster(request=start_cluster_request)
    # Wait for the operation to complete
    start_response = start_operation.result()
    print("Cluster started successfully:", start_response.cluster_name)

# startTheCluster()
