
import os
import csv
from google.cloud import bigquery
from google.cloud import storage

# Set the path to your service account key file
json_path = "C:\\Users\\Nikhil.Dhulekar\\OneDrive - Coforge Limited\\Desktop\\BigDataProject\\hsbcnikhil-21d2598ee1c7.json"
# Set the environment variable to point to your service account key file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = json_path
# Create a BigQuery client
client = bigquery.Client()
# Define your SQL query
query = """
    SELECT CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, view_count
    FROM `bigquery-public-data.stackoverflow.posts_questions`
    WHERE tags like '%google-bigquery%'
    ORDER BY view_count DESC
    LIMIT 10
"""
# Run the query
query_job = client.query(query)
# Open the CSV file for writing
with open("output.csv", "w", newline="") as csvfile:
    # Create a CSV writer object
    csv_writer = csv.writer(csvfile)
    # Write the header row
    csv_writer.writerow(["URL", "View Count"])
    # Fetch the results and write them to the CSV file
    for row in query_job:
        url = row.url
        view_count = row.view_count
        print(f"{url} : {view_count} views")  # Print the results
        # Write the row to the CSV file
        csv_writer.writerow([url, view_count])
# Get the absolute path of the CSV file
absolute_path = os.path.abspath("output.csv")
print("CSV file path:", absolute_path)

