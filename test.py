from google.cloud import storage
import ujson
import pprint
from datetime import datetime

def gather_metadata(bucket, market):
	existing_market = bucket.blob("metadata/" + str(market['ID']))
	if existing_market.exists() is False:
		contracts = market['Contracts']
		del market['Contracts']
		del market['Image']
		storage_location = str(market['ID'])
		blob = bucket.blob("metadata/" + storage_location)
		blob.upload_from_string(ujson.dumps(market))
		for contract in contracts:
			new_dict = {'ID' : contract['ID'], 'LongName' : contract['LongName'], 'Name' : contract['Name'], 'ShortName' : contract['ShortName'], 
			'TickerSymbol' : contract['TickerSymbol'], 'URL': contract['URL']}
			blob = bucket.blob("metadata/" + storage_location + "/" + str(new_dict['ID']))
			blob.upload_from_string(ujson.dumps(new_dict))
	else:
		existing_market_json = ujson.loads(existing_market.download_as_string())
		if market['Status'] != existing_market_json['Status']:
			existing_market_json['CloseDate'] = market['TimeStamp']
			existing_market.upload_from_string(ujson.dumps(existing_market_json))
		for contract in market['Contracts']:
			existing_contract = bucket.blob("metadata/" + str(market['ID']) + "/" + str(contract['ID']))
			if existing_contract.exists():
				print("existing contract")
				continue
			else:
				print('new contract')
				new_dict = {'ID' : contract['ID'], 'LongName' : contract['LongName'], 'Name' : contract['Name'], 'ShortName' : contract['ShortName'], 
				'TickerSymbol' : contract['TickerSymbol'], 'URL': contract['URL']}
				blob = bucket.blob("metadata/" + storage_location + "/" + str(new_dict['ID']))
				blob.upload_from_string(ujson.dumps(new_dict))

def store_data(bucket, market):

	storage_location = "data/" + str(market['ID'])
	print(market.keys())
	print(market['URL'])
	if 'Contracts' in market.keys():
		print('Contracts exist')
		for contract in market['Contracts']:
			sub_folder = storage_location + "/" + str(contract['ID']) + "/" + str(market['TimeStamp'])
			new_dict = {'LastTradePrice' : contract['LastTradePrice'], 'BestBuyYesCost' : contract['BestBuyYesCost'], 
			'BestBuyNoCost' : contract['BestBuyNoCost'], 'BestSellYesCost' : contract['BestSellYesCost'], 'DateEnd' : contract['DateEnd'],
			'BestSellNoCost' : contract['BestSellNoCost']}
			blob = bucket.blob(sub_folder)
			blob.upload_from_string(ujson.dumps(new_dict))
	else:
		print('HERE')


# Define bucket/client where data is stored
storage_client = storage.Client()
bucket_name = "crowd_source"
bucket = storage_client.get_bucket(bucket_name)

# Pull list of "blobs" in the storage client and check to make sure that the metada has been stored
new_blobs = bucket.list_blobs(prefix="raw/")
count = 0

for blob in new_blobs:
	# print(blob.name)
	# date = datetime.strptime(blob.name[4:], '%Y-%m-%dT%H:%M:%S.%f')
	content = blob.download_as_string()
	parsed = ujson.loads(content)
	list_of_markets = parsed['Markets']
	# pull_date = datetime.strptime(list_of_markets[0]['TimeStamp'][:19], '%Y-%m-%dT%H:%M:%S')
	for market in list_of_markets:
		gather_metadata(bucket, market)
		store_data(bucket, market)
	new_blob = bucket.rename_blob(blob, "processed/" + blob.name[4:])
	if count > 10:
		break
	count = count + 1

