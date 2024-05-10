from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name, zone):
    """This sample walks a user through creating a Cloud Dataproc cluster
    using the Python client library.

    Args:
        project_id (string): Project to use for creating resources.
        region (string): Region where the resources should live.
        cluster_name (string): Name to use for creating a cluster.
    """

    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4",
                              "disk_config": {
                                  "boot_disk_size_gb": 50
                              }

                              },
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4",
                              "disk_config": {
                                  "boot_disk_size_gb": 50
                              }

                              },
            "gce_cluster_config": {
                "zone_uri": f"projects/{project_id}/zones/{zone}",
            },
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    # Output a success message.
    print(f"Cluster created successfully: {result.cluster_name}")

    return True

# project_id = 'myprojecthsbc'
# region = 'us-central1'
# cluster_name = 'cluster-1234'
# zone = 'us-central1-f'
# create_cluster(project_id, region, cluster_name, zone)


def delete_cluster(project_id, region, cluster_name):
    # Now, let's delete the cluster
    cluster_delete_request = {
        "project_id": project_id,
        "region": region,
        "cluster_name": "your-cluster-name",
    }
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    delete_operation = cluster_client.delete_cluster(
        request={"project_id": project_id, "region": region, "cluster_name": cluster_name}
    )

    # Wait for the delete operation to complete
    delete_response = delete_operation.result()

    # Print confirmation
    print("Cluster deleted successfully.")

    return True
