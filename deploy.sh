#!/bin/bash
#!/bin/bash
###########################################################################
#
#  Copyright 2021 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

echo "~~~~~~~~ Welcome ~~~~~~~~~~"
echo "Thanks for using GA Serverless Streaming."
echo "---------------------------"
read -p "Please enter your GCP PROJECT ID: " project_id
echo "---------------------------"
echo "~~~~~~~~ Enabling APIs ~~~~~~~~~~"
gcloud services enable \
cloudbuild.googleapis.com \
dataflow.googleapis.com \
compute.googleapis.com \
cloudfunctions.googleapis.com \
logging.googleapis.com \
storage-component.googleapis.com \
storage-api.googleapis.com \
bigquery-json.googleapis.com \
pubsub.googleapis.com \
--async
echo "~~~~~~~~ Creating ga-hits Pub/Sub topic ~~~~~~~~~~"
gcloud pubsub topics create ga-hits
read -p "Please enter your desired FUNCTION NAME. The recommended
function name is 'collect': " function_name
echo "---------------------------"
read -p "Please enter your desired local timezone for your GA data from this
list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones. Use the
format from the column 'TZ database name'. Example timezones - US/Eastern,
US/Pacific, Europe/Vienna: " timezone
echo "~~~~~~~~ Creating function ~~~~~~~~~~"
cd collect_function
gcloud functions deploy $function_name \
	--project $project_id \
	--runtime python38 \
	--memory 256MB \
	--timeout 90s \
	--trigger-http \
	--entry-point collect \
	--allow-unauthenticated \
	--set-env-vars=GCP_PROJECT=$function_name,LOCAL_TIMEZONE=$timezone
cd ..
cd ga_stream_beam
echo "---------------------------"
read -p "Please enter staging bucket name: " staging_bucket_name
echo "~~~~~~~~ Creating Staging Bucket ~~~~~~~~~~"
gsutil mb gs://$staging_bucket_name
echo "---------------------------"
read -p "Please enter the new BigQuery dataset name (recommended: ga_data): " dataset_name
echo "---------------------------"
echo "~~~~~~~~ Creating BigQuery Dataset ~~~~~~~~~~"
bq mk $project_id:$dataset_name
echo "---------------------------"
read -p "Please enter the new BigQuery table name (recommended: ga_hits): " table_name
bq mk -t --time_partitioning_type=DAY $project_id:$dataset_name.$table_name
echo "~~~~~~~~ Installing Apache Beam Python SDK ~~~~~~~~~~"
pip3 install apache-beam[gcp]
echo "~~~~~~~~ Installing Package Dependencies ~~~~~~~~~~"
pip3 install ua-parser
echo "---------------------------"
read -p "Please enter Dataflow job name. The name must consist of only the characters [a-z0-9], starting with a letter and ending with a letter or number: " job_name
echo "---------------------------"
read -p "Please enter Dataflow job region: " job_region
echo "~~~~~~~~ Deploying Pipeline ~~~~~~~~~~"
python3 hit_processing.py \
  --streaming \
  --job_name=$job_name \
  --project=$project_id \
  --setup_file=./setup.py \
  --save_main_session True \
  --input_topic=projects/$project_id/topics/ga-hits \
  --dataset=$dataset_name \
  --table=$table_name \
  --temp_location=gs://$staging_bucket_name/ \
  --staging_location=gs://$staging_bucket_name/ \
  --runner=DataflowRunner \
  --region=$job_region
echo "~~~~~~~~ Serverless GA streaming pipeline deployment complete! ~~~~~~~~~~"
echo "Please update your Google Analytics or GTM implementation to send hits to the pipeline."
