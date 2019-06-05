#!/bin/bash

python main.py \
--setup_file ./setup.py \
--extra_package dist/etl-1.1.1.tar.gz \
--job_name "pickme-events-pipeline" \
--runner "DataflowRunner" \
--project "pickme-uat-staging-210708" \
--temp_location "gs://temp-dataflow-test/tmp/" \
--staging_location "gs://temp-dataflow-test/staging/" \
--output-bucket "temp-dataflow-test" \
--output-bigquery-dataset "test_bigquery" \
--output-bigquery-table "test_table" \
--driver-event-subscription "projects/pickme-uat-staging-210708/subscriptions/driver-events" \
--passenger-event-subscription "projects/pickme-uat-staging-210708/subscriptions/trip-events" \
--trip-event-subscription "projects/pickme-uat-staging-210708/subscriptions/passenger-events" \
--window-size 300 \
--streaming
