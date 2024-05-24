from google.cloud import storage


def UploadFileToGCS(local_file_path, bucket_name, destination_blob_name):
    # flag_value = False

    # Initialize a client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object
    blob = bucket.blob(destination_blob_name)
    print("{} {} ".format("Created blob", blob))
    print("{} {} ".format("blob Exist", blob.exists()))

    # Delete the existing blob if it exists
    if blob.exists():
        blob.delete()

    print("Before uploading file")
    # Upload the local file
    blob.upload_from_filename(local_file_path)
    # flag_value = True

    print(f"File {local_file_path} uploaded as {destination_blob_name} in {bucket_name}.")