# This script can be run on its own:
#   python create_cluster.py ${PROJECT_ID} ${REGION} ${CLUSTER_NAME}

import sys

# [START dataproc_create_cluster]
from google.cloud import dataproc_v1 as dataproc
from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name):
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
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 4, "machine_type_uri": "n1-standard-2"},

        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    # Output a success message.
    print(f"Cluster created successfully: {result.cluster_name}")





# Call functions
#create_cluster()
project_id = 'hsbcnikhil'
region = 'us-central1'
cluster_name = 'myclusterc'
create_cluster(project_id, region, cluster_name)


