from google.cloud import storage

# create storage client
storage_client = storage.Client.from_service_account_json('C:\\Users\\Nikhil.Dhulekar\\OneDrive - Coforge Limited\\Desktop\\BigDataProject\\hsbcnikhil-f724d551ea1b.json')
bucket = storage_client.get_bucket('**BUCKET NAME**')
# get bucket data as blob
blob = bucket.blob('**SPECIFYING THE DOXC FILENAME**')
downloaded_blob = blob.download_as_string()
downloaded_blob = downloaded_blob.decode("utf-8")
print(downloaded_blob)