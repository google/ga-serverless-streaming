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
"""Reads hits from Pub/Sub and streams to BigQuery."""


import argparse
import json
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from modules import hit_schema
from modules.hit_schema import get_schema
from ua_parser import user_agent_parser
from urllib.parse import urlparse
from urllib.parse import parse_qsl
from datetime import datetime


class FormatHit(beam.DoFn):
  """Extracts from Measurement Protocol parameters and formats into a hit."""

  def build_hit(self, payload):
    """Populates an object matching the table schema."""

    ua = self.parse_ua(payload)
    url = urlparse(payload.get('dl'))
    return {
        'serverTimeUtc':
            payload.get('serverTimeUtc'),
        'clientId':
            payload.get('cid'),
        'userId':
            payload.get('uid'),
        'hitType':
            payload.get('t').upper(),
        'isInteraction':
            False if payload.get('ni') == '1' else True,
        'customDimensions':
            self.set_custom_dimensions(
                payload,
                [key for key in payload if re.match(r'cd\d{1,3}', key)]),
        'customMetrics':
            self.set_custom_metrics(
                payload,
                [key for key in payload if re.match(r'cm\d{1,3}', key)]),
        'page': {
            'hostname':
                url.hostname,
            'pagePath':
                payload['dp'] if payload.get('dp') else url.path,
            'pageTitle':
                payload.get('dt'),
            'url':
                payload.get('dl'),
            'query':
                url.query,
            'referrer':
                payload['dr'] if payload.get('dr') else payload.get('referrer'),
            'linkId':
                payload.get('linkId')
        },
        'eventInfo': {
            'eventCategory': payload.get('ec'),
            'eventAction': payload.get('ea'),
            'eventLabel': payload.get('el'),
            'eventValue': int(payload['ev']) if payload.get('ev') else None
        },
        'promotion':
            self.set_promos(
                payload,
                [key for key in payload if re.match(r'promo\d{1,3}id', key)]),
        'promotionAction':
            self.set_promo_action(payload),
        'product':
            self.set_products(
                payload,
                [key for key in payload if re.search(r'p[r|i]\d{1,3}id', key)]),
        'ecommerceAction':
            self.set_ecommerce_action(payload),
        'transaction': {
            'transactionId':
                payload.get('ti'),
            'affiliation':
                payload.get('ta'),
            'transactionRevenue':
                float(payload['tr']) if payload.get('tr') else None,
            'transactionTax':
                float(payload['tt']) if payload.get('tt') else None,
            'transactionShipping':
                float(payload['ts']) if payload.get('ts') else None,
            'transactionCoupon':
                payload.get('tcc'),
            'currencyCode':
                payload.get('cu')
        },
        'trafficSource':
            self.set_traffic_source(payload, url),
        'device': {
            'screenColors':
                payload.get('sd'),
            'screenResolution':
                payload.get('sr'),
            'browserSize':
                payload.get('vp'),
            'javaEnabled':
                False if payload.get('je') == '0' else True,
            'language':
                payload.get('ul'),
            'documentEncoding':
                payload.get('de'),
            'flashVersion':
                payload.get('fv'),
            'browser':
                self.set_ua_data(ua, 'browser'),
            'browserVersion':
                self.set_ua_data(ua, 'browserVersion'),
            'mobileDeviceModel':
                self.set_ua_data(ua, 'mobileDeviceModel'),
            'operatingSystem':
                self.set_ua_data(ua, 'operatingSystem'),
            'operatingSystemVersion':
                 self.set_ua_data(ua, 'operatingSystemVersion'),
            'mobileDeviceBranding':
                self.set_ua_data(ua, 'mobileDeviceBranding'),
            'userAgent':
                payload.get('userAgent')
        },
        'geo': {
            'country': payload.get('country'),
            'region': payload.get('region'),
            'city': payload.get('city')
        },
        'latencyTracking': {
            'pageLoadTime':
                int(payload.get('plt')) if payload.get('plt') else None,
            'pageDownloadTime':
                int(payload.get('pdt')) if payload.get('pdt') else None,
            'domainLookupTime':
                int(payload.get('dns')) if payload.get('dns') else None,
            'redirectionTime':
                int(payload.get('rrt')) if payload.get('rrt') else None,
            'serverResponseTime':
                int(payload.get('srt')) if payload.get('srt') else None,
            'serverConnectionTime':
                int(payload.get('tcp')) if payload.get('tcp') else None,
            'domInteractiveTime':
                int(payload.get('dit')) if payload.get('dit') else None,
            'domContentLoadedTime':
                int(payload.get('clt')) if payload.get('clt') else None
        },
        'experimentId':
            payload.get('xid'),
        'experimentVariant':
            payload.get('xvar'),
        'trackingId':
            payload.get('tid'),
        'containerId':
            payload.get('gtm'),
        'tagVersion':
            payload.get('v'),
        'cacheBuster':
            payload.get('z')
    }

  def parse_ua(self, payload):
    """Parses the user agent string."""
    try:
      return user_agent_parser.Parse(payload.get('userAgent'))
    except:
      return None

  def set_ua_data(self, ua, field):
    """Sets user agent dimensions."""
    try:
      if ua != None:
        if field == 'browser':
          return (ua['user_agent']['family'] or None)
        elif field == 'browserVersion':
          return (ua['user_agent']['major'] or '') + '.' + (ua['user_agent']['minor'] or '') + '.' + (ua['user_agent']['patch'] or '')
        elif field == 'mobileDeviceModel':
          return (ua['device']['model'] or None)
        elif field == 'operatingSystem':
          return (ua['os']['family'] or None)
        elif field == 'operatingSystemVersion':
          return (ua['os']['major'] or '') + '.' + (ua['os']['minor'] or '') + '.' + (ua['os']['patch'] or '')
        elif field == 'mobileDeviceBranding':
          return (ua['device']['brand'] or None)
      else:
        return None
    except:
      return None


  def set_custom_dimensions(self, payload, keys):
    """Sets custom dimensions."""
    definitions = []
    for key in keys:
      definition = {
          'index': int(re.search(r'cd([0-9]+)', key).group(1)),
          'value': payload.get(key)
      }
      definitions.append(definition)
    return definitions

  def set_custom_metrics(self, payload, keys):
    """Sets custom metrics."""
    definitions = []
    for key in keys:
      definition = {
          'index': int(re.search(r'cm([0-9]+)', key).group(1)),
          'value': payload.get(key)
      }
      definitions.append(definition)
    return definitions

  def set_promos(self, payload, keys):
    """Sets promotions."""
    promos = []
    for key in keys:
      promo_index = int(re.search(r'promo([0-9]+)', key).group(1))
      promo = {
          'promoIndex': promo_index,
          'promoId': payload.get(key),
          'promoName': payload.get('promo' + str(promo_index) + 'nm'),
          'promoCreative': payload.get('promo' + str(promo_index) + 'cr'),
          'promoPosition': payload.get('promo' + str(promo_index) + 'ps')
      }
      promos.append(promo)
    return promos

  def set_promo_action(self, payload):
    """Sets promotion action."""
    if payload.get('promoa'):
      return {
          'promoIsView': False if payload.get('promoa') == 'click' else True,
          'promoIsClick': True if payload.get('promoa') == 'click' else False
      }
    elif [key for key in payload if re.match(r'promo\d{1,3}id', key)]:
      return {'promoIsView': True, 'promoIsClick': False}
    else:
      return None

  def set_products(self, payload, keys):
    """Sets products."""
    products = []
    for key in keys:
      is_impression = None
      list_index = None
      product_index = None
      if re.match(r'il', key):
        is_impression = True
        list_index = re.search(r'il([0-9]+)', key).group(1)
        product_index = re.search(r'pi([0-9]+)', key).group(1)
      else:
        is_impression = False
        product_index = re.search(r'pr([0-9]+)', key).group(1)
      product = {
          'productIndex':
              int(product_index),
          'productSKU':
              self.set_product_metadata('id', payload, product_index,
                                        list_index, is_impression),
          'productName':
              self.set_product_metadata('nm', payload, product_index,
                                        list_index, is_impression),
          'productBrand':
              self.set_product_metadata('br', payload, product_index,
                                        list_index, is_impression),
          'productCategory':
              self.set_product_metadata('ca', payload, product_index,
                                        list_index, is_impression),
          'productVariant':
              self.set_product_metadata('va', payload, product_index,
                                        list_index, is_impression),
          'productPrice':
              self.set_product_metadata('pr', payload, product_index,
                                        list_index, is_impression),
          'productQuantity':
              payload.get('pr' + str(product_index) + 'qt'),
          'productCouponCode':
              payload.get('pr' + str(product_index) + 'cc'),
          'customDimensions':
              self.set_custom_dimensions(payload, [
                  k for k in payload
                  if re.search(r'p[r|i]' + re.escape(product_index) + r'cd', k)
              ]),
          'customMetrics':
              self.set_custom_metrics(payload, [
                  k for k in payload
                  if re.search(r'p[r|i]' + re.escape(product_index) + r'cm', k)
              ]),
          'productListName':
              payload.get('il' + list_index + 'nm')
              if is_impression else payload.get('pal'),
          'productListPosition':
              self.set_product_metadata(
              'ps', payload, product_index,list_index, is_impression),
          'isImpression':
              is_impression,
          'isClick':
              True if payload.get('pa') == 'click' else False
      }
      products.append(product)
    return products

  def set_product_metadata(self, param, payload, product_index,
                           list_index, is_impression):
    """Sets product metadata."""
    try:
      metadata = None
      if is_impression:
        metadata = payload.get('il' + str(list_index) + 'pi' + str(product_index) + param)
      else:
        metadata = payload.get('pr' + str(product_index) + param)
      return metadata
    except:
      return None

  def set_ecommerce_action(self, payload):
    """Sets ecommerce action."""
    action_type = payload.get('pa')
    types = {
        'click': 1,
        'detail': 2,
        'add': 3,
        'remove': 4,
        'checkout': 5,
        'purchase': 6,
        'refund': 7,
        'checkout_option': 8
    }
    if action_type:
      type_number = types.get(action_type)
      return {
          'action_type': type_number,
          'name': action_type,
          'step': int(payload['cos']) if payload.get('cos') else None,
          'option': payload.get('col')
      }
    else:
      return None

  def set_traffic_source(self, payload, url):
    """Sets traffic source."""

    params = dict(parse_qsl(url.query))
    if params.get('utm_source'):
      return {
          'campaign':
              params.get('utm_campaign')
              if params.get('utm_campaign') else payload.get('cn'),
          'campaignCode':
              params.get('utm_id')
              if params.get('utm_id') else payload.get('ci'),
          'source':
              params.get('utm_source')
              if params.get('utm_source') else payload.get('cs'),
          'medium':
              params.get('utm_medium')
              if params.get('utm_medium') else payload.get('cm'),
          'keyword':
              params.get('utm_keyword')
              if params.get('utm_keyword') else payload.get('ck'),
          'adContent':
              params.get('utm_content')
              if params.get('utm_content') else payload.get('cc'),
          'gclId':
              params.get('gclid')
              if params.get('gclid') else payload.get('gclid'),
          'dclId':
              params.get('dclid')
              if params.get('dclid') else payload.get('dclid'),
          'referrer':
              payload['dr'] if payload.get('dr') else payload.get('referrer'),
          'gclSrc':
              params['gclsrc']
              if params.get('gclsrc') else payload.get('gclsrc')
      }
    else:
      return {
          'campaign':
              payload.get('cn'),
          'campaignCode':
              payload.get('ci'),
          'source':
              payload.get('cs'),
          'medium':
              payload.get('cm'),
          'keyword':
              payload.get('ck'),
          'adContent':
              payload.get('cn'),
          'gclId':
              payload.get('gclid'),
          'dclId':
              payload.get('dclid'),
          'referrer':
              payload['dr'] if payload.get('dr') else payload.get('referrer'),
          'gclSrc':
              payload.get('gclsrc')
      }

  def process(self, elem):
    payload = json.loads(elem)
    hit = self.build_hit(payload)
    yield hit


def run(argv=None):
  """Define and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input_topic',
      dest='input_topic',
      required=True,
      help='Cloud Pub/Sub topic to read from.')
  parser.add_argument(
      '--project',
      dest='project_id',
      required=True,
      help='Google Cloud Project ID.')
  parser.add_argument(
      '--dataset',
      dest='dataset',
      required=True,
      help='BigQuery dataset to write to.')
  parser.add_argument(
      '--table',
      dest='table',
      default='hits',
      help='Prefix for BigQuery daily tables to write to. '
      '_YYYYMMDD is appended to the table prefix.')
  args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend(['--project=' + args.project_id])
  options = PipelineOptions(pipeline_args)

  with beam.Pipeline(options=options) as p:
    (p | 'ReadTopic' >> beam.io.ReadFromPubSub(topic=args.input_topic)
     | 'FormatHit' >> beam.ParDo(FormatHit())
     | 'WriteHit' >> beam.io.WriteToBigQuery(
         args.table,
         args.dataset,
         schema=get_schema(),
         create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
