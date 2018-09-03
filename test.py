from google.cloud import storage
import json
import pprint

def gather_metadata(bucket, market):
	stored_markets = bucket.list_blobs(prefix="metadata/")
	if market['ID'] not in stored_markets:
		pp = pprint.PrettyPrinter(indent=2)
		contracts = market['Contracts']
		del market['Contracts']
		del market['Image']
		storage_location = market['ID']
		for contract in contracts:
			pp.pprint(contract)

	else:
		metadata = bucket.blob(market['ID'])
		print('here')

# Define bucket/client where data is stored
storage_client = storage.Client()
bucket_name = "crowd_source"
bucket = storage_client.get_bucket(bucket_name)

# Pull list of "blobs" in the storage client and check to make sure that the metada has been stored
new_blobs = bucket.list_blobs(prefix="raw/")

for blob in new_blobs:
	content = blob.download_as_string()
	parsed = json.loads(content)
	list_of_markets = parsed['Markets']
	for market in list_of_markets:
		gather_metadata(bucket, market)
		break
	break

