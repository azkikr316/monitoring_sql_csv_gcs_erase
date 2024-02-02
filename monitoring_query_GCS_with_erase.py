'''
Created By: Romel Mendoza
Date: 2 Febraury 2024

This script integrates the database query, CSV file creation, and the GCS upload process into a single workflow, 
utilizing the upload_to_gcs function you confirmed as working for uploading files to GCS. This should resolve 
any issues related to file uploads by leveraging a method proven to work in your environment.
Includes a line to delete the local CSV file after it has been successfully uploaded to the GCS bucket. 
The os.remove() function is called with the source_file_name (the full path to the local CSV file), removing the 
file from the local filesystem.
'''
import os
import pandas as pd
import pymysql
from google.cloud import storage
from datetime import datetime

# Function to upload file to Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """
    Uploads a file to the specified bucket in Google Cloud Storage.
    Parameters:
    - bucket_name: Name of the GCS bucket
    - source_file_name: Path to the local file to upload
    - destination_blob_name: Name for the file in GCS, including the folder path
    """
    # Create a client instance
    client = storage.Client()

    # Get the bucket object
    bucket = client.bucket(bucket_name)

    # Create a blob object from the file
    blob = bucket.blob(destination_blob_name)

    # Upload the file
    blob.upload_from_filename(source_file_name)

    print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")

    # Remove the file from local directory after successful upload
    os.remove(source_file_name)
    print(f"Local file {source_file_name} has been deleted.")

def query_to_csv_and_upload(bucket_name, local_directory):
    # Database connection
    connection = pymysql.connect(host='127.0.0.1', user='root', password='hiroodaikai01', db='drums')

    # SQL query to fetch records from the last 480 minutes
    sql_query = "SELECT * FROM monitoring WHERE TimeStamp >= NOW() - INTERVAL 480 MINUTE;"

    # Execute query
    df = pd.read_sql(sql_query, connection)

    # Generate filename with current datetime
    datetime_format = datetime.now().strftime('%Y%m%d%H%M%S')
    csv_file_name = f'monitoring-{datetime_format}.csv'
    csv_file_path = os.path.join(local_directory, csv_file_name)

    # Convert to CSV
    df.to_csv(csv_file_path, index=False)

    # Upload the file
    destination_blob_name = f"drums/{csv_file_name}"  # Prepend the folder path to the file name
    upload_to_gcs(bucket_name, csv_file_path, destination_blob_name)

# Set Google Cloud credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/devdb/poised-runner-396506-ad1cdb4cec46.json'

# Specify the bucket name
bucket_name = 'rockset'
# Specify the local directory to temporarily store the CSV file
local_directory = '/tmp'

# Perform the query and upload
query_to_csv_and_upload(bucket_name, local_directory)
