#!/usr/bin/env bash
export PROJCET="$(gcloud config get project)"
export PROJECT_NUMBER="$(gcloud projects list --format json | jq -r --arg prj $PROJCET '.[]|select(.projectId==$prj)|.projectNumber')"
export DS="test_ds"
export RECORDS_FILE="records.json"
export SCHEMA="taxi_rides_strings"
export TOPIC="taxirides-realtime" # this should be created with schema by topic2topic
export TABLE="${DS}.${SCHEMA}"
CREATE_TOPIC="FALSE"
export pattern_to_delete="taxi" # this will limit the cleanup to resources matching taxi anywhere!
cat policy.json | envsubst > mypolicy.json
test="$(jq '.' mypolicy.json)"
[[ $? -ne 0 ]] && echo failed to create mypolicy.json you may need to create it manually
echo > logs/test.log
start_pubsub_bq() {
  [[ "$CREATE_TOPIC" == "TRUE" ]] \
  && gcloud pubsub schemas create --definition-file=schemas/${SCHEMA}.pb --type protocol-buffer ${SCHEMA}   >>logs/test.log 2>&1 \
  && gcloud pubsub topics create ${TOPIC} --schema ${SCHEMA} --message-encoding json   >>logs/test.log 2>&1

  bq mk $DS   >>logs/test.log 2>&1
  bq mk -t --schema schemas/${SCHEMA}_table.json ${TABLE}   >>logs/test.log 2>&1
  bq set-iam-policy ${TABLE}  mypolicy.json   >>logs/test.log 2>&1
  gcloud pubsub subscriptions create ${TOPIC}_sub --bigquery-table="${PROJCET}:${TABLE}" --topic ${TOPIC} --drop-unknown-fields --use-topic-schema --write-metadata
  echo "Events should be now published to local topic"
}

cleanup() {
  pid=$(ps -ef | grep "python ./topic2topic" | grep -v grep | awk '{print $1}')
  [[ -n $pid ]] && kill -9 $(ps -ef | grep "python ./topic2topic" | grep -v grep | awk '{print $1}')
  gcloud pubsub topics list --format json | jq -r '.[].name' | grep "$pattern_to_delete" | xargs -n1 gcloud pubsub topics delete   >>logs/test.log 2>&1
  gcloud pubsub subscriptions list --format json | jq -r '.[].name' | grep "$pattern_to_delete"  | xargs -n1 gcloud pubsub subscriptions delete    >>logs/test.log 2>&1
  gcloud pubsub schemas list --format json | jq -r '.[].name' | grep "$pattern_to_delete" | xargs -n1 gcloud pubsub schemas delete --quiet   >>logs/test.log 2>&1
  echo Y | bq rm -r ${DS}   >>logs/test.log 2>&1
}

validate_messages() {
  records="${2:-records.json}"
  [[ ! -f $records ]] && echo "Can not validate \"$records\" file not found!" && return 1
  for i in $(jq '.' -c "$records")
  do
    gcloud pubsub schemas validate-message  --message-encoding=json --schema-name ${1:-taxi_rides} --message "$i"
  done
}

query_bq() {
  bq query 'select * from '${TABLE}';'
}