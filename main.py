# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START app]
import logging

from google.appengine.api import urlfetch
from google.cloud import storage

from flask import Flask, request


app = Flask(__name__)



@app.route('/')
def hello():
    return 'Hello World!'

@app.route('/predictit')
def collect_market_data():
    is_cron = request.headers.get('X-Appengine-Cron', False)
    print("````````````````````````")
    # if not is_cron:
    #     print("HUR")
    #     return 'Bad Request', 400
    try:
        print("````````````````````````")
        response = urlfetch.fetch("https://www.predictit.org/api/marketdata/all/", headers={"Accept": "application/json"})
        print(response.content)
        print("~~~~~~~~~~~~~~~~~~~~~~~~")
        return "Finished try", 200
    except Exception as e:
        logging.exception(e)
        return "Error: <pre>{}</pre>".formate(e), 500


@app.errorhandler(500)
def server_error(e):
    # Log the error and stacktrace.
    logging.exception('An error occurred during a request.')
    return 'An internal error occurred.', 500
# [END app]