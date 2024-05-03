from google.cloud import storage
import csv


def compare_csv_files(bucket_name, file1_name, file2_name):
    # Initialize GCS client
    client = storage.Client()

    # Get the bucket containing the CSV files
    bucket = client.bucket(bucket_name)

    # Download the CSV files as blobs
    blob1 = bucket.blob(file1_name)
    blob2 = bucket.blob(file2_name)

    # Download the blobs to local files
    blob1.download_to_filename('/tmp/file1.csv')
    blob2.download_to_filename('/tmp/file2.csv')

    # Compare the CSV files
    differences = []
    with open('/tmp/file1.csv', 'r') as file1, open('/tmp/file2.csv', 'r') as file2:
        reader1 = csv.reader(file1)
        reader2 = csv.reader(file2)
        for row1, row2 in zip(reader1, reader2):
            if row1 != row2:
                differences.append((row1, row2))

    return differences


if __name__ == "__main__":
    # Replace these values with your GCS bucket name and CSV file names
    bucket_name = 'dumybucket123'
    file1_name = 'DB2data.csv'
    file2_name = 'output.csv'

    differences = compare_csv_files(bucket_name, file1_name, file2_name)

    if differences:
        print("Differences found:")
        for difference in differences:
            print(f"File 1: {difference[0]}")
            print(f"File 2: {difference[1]}")
    else:
        print("No differences found.")