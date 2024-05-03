from google.cloud import dataproc_v1 as dataproc
import os
# Set the path to your service account key file

json_path = "C:\\Users\\Nikhil.Dhulekar\\OneDrive - Coforge Limited\\Desktop\\BigDataProject\\hsbcnikhil-f724d551ea1b.json"

# Set the environment variable to point to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path

# Set your project ID, region, and other parameters
project_id = 'hsbcnikhil'
region = 'africa-south1'
cluster_name = 'my-cluster4'
bucket = 'datacompare3'
job_file_uri = 'gs://DataCompare/job.py'


# Create a cluster
def create_cluster():
    client = dataproc.ClusterControllerClient()
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n2-standard-4",
                              "disk_config": {"boot_disk_size_gb": 250}},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n2-standard-2",
                              "disk_config": {"boot_disk_size_gb": 250}},
            "config_bucket": bucket,
            "software_config": {"image_version": "2.1-debian11"}
        }
    }
    op = client.create_cluster(project_id=project_id, region=region, cluster=cluster)
    result = op.result()
    print("Cluster created:", result.cluster_name)


# List clusters
def list_clusters():
    client = dataproc.ClusterControllerClient()
    clusters = client.list_clusters(project_id=project_id, region=region)
    for cluster in clusters:
        print(cluster.cluster_name, cluster.status.state)


# Delete cluster
def delete_cluster():
    client = dataproc.ClusterControllerClient()
    op = client.delete_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
    op.result()
    print("Cluster deleted")


# Submit PySpark job to cluster
def submit_pyspark_job():
    client = dataproc.JobControllerClient()
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": job_file_uri},
    }
    op = client.submit_job(project_id=project_id, region=region, job=job)
    result = op.result()
    print("Job submitted:", result.reference.job_id)


# List Dataproc jobs
def list_dataproc_jobs():
    client = dataproc.JobControllerClient()
    jobs = client.list_jobs(project_id=project_id, region=region)
    for job in jobs:
        print(job.reference.job_id, job.status.state)


# Call functions
create_cluster()
list_clusters()
submit_pyspark_job()
list_dataproc_jobs()
