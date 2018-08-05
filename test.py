from google.cloud import storage

storage_client = storage.Client()
bucket_name = "crowd_source"
bucket = storage_client.get_bucket(bucket_name)
