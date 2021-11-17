# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Collects a GA hit payload and pushes it to Pub/Sub."""

import json
import logging
import os
import time

from urllib import parse
from flask import abort
from flask import make_response
from google.cloud import pubsub
from datetime import datetime


publisher_client = pubsub.PublisherClient()
project_id = os.environ.get('GCP_PROJECT')
local_timezone = os.environ.get('LOCAL_TIMEZONE')
with open('config.json') as f:
  data = f.read()
config = json.loads(data)


def collect(request):
  """This method is exported and handles POST requests.

  Requests contain a URL-encoded query string (GA hit payload) in the body,
  with a 'text/plain' content type. The hit payload is transformed into a
  dict and enriched with selected request headers. The final payload is
  converted into an encoded JSON string and published to the Pub/Sub topic.

  Args:
    request: A Flask request object.

  Returns:
    A Flask response object.

  Raises:
    ValueError: An error occurred parsing the hit payload.
  """
  response = make_response()  # Create a response to set the CORS headers.
  response.headers['Access-Control-Allow-Origin'] = '*'
  response.headers['Access-Control-Allow-Methods'] = 'POST'

  # GA rejects payloads larger than 8192 bytes so the content length
  # limit uses that value and adds room for additional parameters that
  # may be manually added.
  if request.content_length and request.content_length > 8192 + 3000:
    return abort(413)
  if request.method != 'POST':
    return abort(405)
  if request.content_type != 'text/plain':
    return abort(415)

  # Get the payload as text from the request body.
  raw_payload = request.get_data(as_text=True)

  # Parse the hit payload as a query string - parameters with empty
  # values are retained because 'document.referrer', which may have
  # an empty value, is manually added to the payload client-side.
  # analytics.js does not produce the same query string parameter
  # twice - so we consider the conversion into a dictionary to be safe.
  try:
    payload = dict(parse.parse_qsl(
        raw_payload, keep_blank_values=True, strict_parsing=True))
  except ValueError as e:
    logging.exception('Error parsing hit payload: %s', e)

  # Add timestamp, local timezone datetime, IP, UA, and geo headers.
  payload['serverTimeUtc'] = int(time.time())
  payload['ipAddress'] = request.headers.get('X-Appengine-User-Ip')
  payload['userAgent'] = request.headers.get('User-Agent')
  payload['country'] = request.headers.get('X-AppEngine-Country')
  payload['region'] = request.headers.get('X-AppEngine-Region')
  payload['city'] = request.headers.get('X-AppEngine-City')

  # Publish to the topic.
  topic_path = publisher_client.topic_path(project_id, config['TOPIC'])
  publisher_client.publish(topic_path, data=json.dumps(payload).encode('utf-8'))

  # Respond to the client.
  return response
