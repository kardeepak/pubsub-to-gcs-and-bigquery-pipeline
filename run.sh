#!/bin/bash

python main.py \
--setup_file ./setup.py \
--extra_package dist/etl-1.1.1.tar.gz \
--job_name "pickme-events-pipeline" \
--runner "DirectRunner" \
--project "pickme-uat-staging-210708" \
--output-bigquery-dataset "test_bigquery" \
--output-bigquery-table "test_table" \
--driver-event-subscription "projects/pickme-uat-staging-210708/subscriptions/random" \
--passenger-event-subscription "projects/pickme-uat-staging-210708/subscriptions/random1" \
--trip-event-subscription "projects/pickme-uat-staging-210708/subscriptions/random2" \
--window-size 10 \
--streaming
