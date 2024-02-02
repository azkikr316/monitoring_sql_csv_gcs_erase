This script integrates the database query, CSV file creation, and the GCS upload process into a single workflow, 
utilizing the upload_to_gcs function you confirmed as working for uploading files to GCS. This should resolve 
any issues related to file uploads by leveraging a method proven to work in your environment.
Includes a line to delete the local CSV file after it has been successfully uploaded to the GCS bucket. 
The os.remove() function is called with the source_file_name (the full path to the local CSV file), removing the 
file from the local filesystem.
