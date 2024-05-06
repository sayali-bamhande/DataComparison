from pythonProject.utils.StartCluster import startTheCluster
from pythonProject.utils.StopCluster import stopTheCluster
from pythonProject.utils.SubmitJobToCluster import SubmitJobToCluster
from pythonProject.utils.UploadFileToGCS import upload_to_gcs
from pythonProject.utils.ConstantsData import region, project_id, cluster_name, bucket_name


#local_file_path = "C:\\Users\\Sayali.Bamhande\\PycharmProjects\\pythonProject\\pythonProject\\utils\\CompareCSVAndGenerateOutput.py"
# destination_blob_name = f'CsvDataComparisonUsingDataframe.py'
destination_blob_name = f'CompareCSVAndGenerateOutput.py'
#upload_to_gcs(local_file_path, bucket_name, destination_blob_name)
startTheCluster(project_id,region,cluster_name)
zone = 'us-central1-f'
SubmitJobToCluster(project_id,region,cluster_name,'CompareCSVAndGenerateOutput.py')
stopTheCluster(project_id,region,cluster_name)




