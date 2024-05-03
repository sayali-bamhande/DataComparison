
import sys

# [START dataproc_create_cluster]
from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name,zone):

    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
            "config": {"gce_cluster_config": {"zone_uri": f"projects/{project_id}/zones/{zone}"},
        "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
        "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        "disk_config": {"boot_disk_size_gb": 50},
        "software_config":{"image_version":"2.0.97-debian10","optional_components":{"JUPYTER","ANACONDA"}},
        "initialization_actions":{"executable_file" : "gs://dataproc-initialization-actions/python/pip-install.sh"},
        "gce_cluster_config": {"metadata": "PIP_PACKAGES=google-cloud-storage,spark-nlp==2.5.3"},
        "endpoint_config": {"enable_http_port_access":True},




        # "gce_cluster_config": {
        #     "zone_uri":f"projects/{project_id}/zones/{zone}"
        #     },
        #
        #     "master_config": {
        #         "num_instances": 1,
        #         "machine_type_uri": "n1-standard-2"
        #     },
        #     "worker_config": {
        #         "num_instances": 2,
        #         "machine_type_uri": "n1-standard-2",
        #         "disk_config": {"boot_disk_size_gb": 50}


        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster,zone :"zone"}
    )
    result = operation.result()

    # Output a success message.
    print(f"Cluster created successfully: {result.cluster_name}")
    # [END dataproc_create_cluster]



# if len(sys.argv) < 4:
#     print(f"ARGS VALUE >>> : {sys.argv}")
#     print(f"ARGS >>> : {len(sys.argv)}")
#     sys.exit("python create_cluster.py project_id region cluster_name")

project_id = "hsbcnikhil"
region = "asia-east1"
cluster_name = "demotest"
zone = "asia-east1-b"
create_cluster(project_id, region, cluster_name,zone)