from google.cloud import dataproc_v1


def stopTheCluster(project_id, region, cluster_name):
    client_options = {"api_endpoint": "us-central1-dataproc.googleapis.com:443"}
    client = dataproc_v1.ClusterControllerClient(client_options=client_options)
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

# stopTheCluster()
