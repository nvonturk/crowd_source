from google.appengine.api import urlfetch
from google.cloud import storage
from pytz import reference
from flask import Flask, request

import requests_toolbelt.adapters.appengine
import json
import logging

import pprint
from datetime import datetime

app = Flask(__name__)

requests_toolbelt.adapters.appengine.monkeypatch()

storage_client = storage.Client()
bucket_name = "crowd_source"
bucket = storage_client.get_bucket(bucket_name)
localtime = reference.LocalTimezone()

def gather_metadata(bucket, market):
    existing_market = bucket.blob("metadata/" + str(market['ID']))
    if existing_market.exists() is False:
        contracts = market['Contracts']
        del market['Contracts']
        del market['Image']
        storage_location = str(market['ID'])
        blob = bucket.blob("metadata/" + storage_location)
        blob.upload_from_string(json.dumps(market))
        for contract in contracts:
            new_dict = {'ID' : contract['ID'], 'LongName' : contract['LongName'], 'Name' : contract['Name'], 'ShortName' : contract['ShortName'], 
            'TickerSymbol' : contract['TickerSymbol'], 'URL': contract['URL']}
            blob = bucket.blob("metadata/" + storage_location + "/" + str(new_dict['ID']))
            blob.upload_from_string(json.dumps(new_dict))
    else:
        existing_market_json = json.loads(existing_market.download_as_string())
        if market['Status'] != existing_market_json['Status']:
            existing_market_json['CloseDate'] = market['TimeStamp']
            existing_market.upload_from_string(json.dumps(existing_market_json))
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
                blob.upload_from_string(json.dumps(new_dict))

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
            blob.upload_from_string(json.dumps(new_dict))
    else:
        print('HERE')



@app.route('/')
def hello():
    return 'Hello World!'

@app.route('/predictit')
def collect_market_data():
    is_cron = request.headers.get('X-Appengine-Cron', False)
    if not is_cron:
        print("HUR")
        return 'Bad Request', 400
    try:
        print("````````````````````````")
        response = urlfetch.fetch("https://www.predictit.org/api/marketdata/all/", headers={"Accept": "application/json"})
        parsed = json.loads(response.content)
        blob_name = datetime.now().strftime("%Y-%m-%d %H:%M:%S, " + localtime.tzname(datetime.now()))
        blob_name = "raw/" + blob_name
        blob = bucket.blob(blob_name)
        blob.upload_from_string(response.content, content_type='application/json')
        return "Finished try", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".formate(e), 500

@app.route('/process')
def process_market_date():
    # Define bucket/client where data is stored
    storage_client = storage.Client()
    bucket_name = "crowd_source"
    bucket = storage_client.get_bucket(bucket_name)

    # Pull list of "blobs" in the storage client and check to make sure that the metada has been stored
    new_blobs = bucket.list_blobs(prefix="raw/")

    for blob in new_blobs:
        # print(blob.name)
        # date = datetime.strptime(blob.name[4:], '%Y-%m-%dT%H:%M:%S.%f')
        content = blob.download_as_string()
        parsed = json.loads(content)
        list_of_markets = parsed['Markets']
        # pull_date = datetime.strptime(list_of_markets[0]['TimeStamp'][:19], '%Y-%m-%dT%H:%M:%S')
        for market in list_of_markets:
            gather_metadata(bucket, market)
            store_data(bucket, market)
        new_blob = bucket.rename_blob(blob, "processed/" + blob.name[4:])
        break

@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500
