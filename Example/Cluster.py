# Import the Dataproc client library.
from google.cloud import dataproc

# Create a Dataproc client.
client = dataproc.Client()

# Create a cluster configuration.
cluster_config = dataproc.ClusterConfig()
cluster_config.master_group_config.num_instances = 1
cluster_config.worker_group_config.num_instances = 2

# Create a cluster.
cluster = client.create_cluster(
    project_id="my-project",
    region="us-central1",
    cluster_name="my-cluster",
    cluster_config=cluster_config,
)

# Wait for the cluster to create.
cluster.wait_for_operation()

# Print the cluster name.
print(cluster.cluster_name)