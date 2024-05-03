from pythonProject.utils.CreateClusterTest import create_cluster, delete_cluster
from pythonProject.utils.SubmitJobToCluster import SubmitJobToCluster
from pythonProject.utils.UploadFileToGCS import upload_to_gcs
from pythonProject.utils.ConstantsData import region, project_id, cluster_name, bucket_name


local_file_path = ("C:\\Users\\Sayali.Bamhande\\PycharmProjects\\pythonProject\\pythonProject\\utils"
                   "\\CompareCSVAndGenerateOutput.py")
destination_blob_name = f'CompareCSVAndGenerateOutput.py'

upload_to_gcs(local_file_path, bucket_name, destination_blob_name)
zone = 'us-central1-f'
create_cluster(project_id, region, cluster_name, zone)
SubmitJobToCluster(project_id,region,cluster_name,'CompareCSVAndGenerateOutput.py')
delete_cluster(project_id, region, cluster_name)




