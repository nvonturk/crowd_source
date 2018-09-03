from google.appengine.api import urlfetch
from google.cloud import storage
from pytz import reference
from flask import Flask, request

import requests_toolbelt.adapters.appengine
import json
import logging
import datetime

app = Flask(__name__)

requests_toolbelt.adapters.appengine.monkeypatch()

storage_client = storage.Client()
bucket_name = "crowd_source"
bucket = storage_client.get_bucket(bucket_name)
localtime = reference.LocalTimezone()

@app.route('/')
def hello():
    return 'Hello World!'

@app.route('/predictit')
def collect_market_data():
    is_cron = request.headers.get('X-Appengine-Cron', False)
    # if not is_cron:
    #     print("HUR")
    #     return 'Bad Request', 400
    try:
        print("````````````````````````")
        response = urlfetch.fetch("https://www.predictit.org/api/marketdata/all/", headers={"Accept": "application/json"})
        parsed = json.loads(response.content)
        blob_name = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S, " + localtime.tzname(datetime.datetime.now()))
        blob_name = "raw/" + blob_name
        blob = bucket.blob(blob_name)
        blob.upload_from_string(response.content, content_type='application/json')
        return "Finished try", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".formate(e), 500


@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500
