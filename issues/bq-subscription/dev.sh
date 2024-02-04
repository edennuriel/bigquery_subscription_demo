#!/usr/bin/env bash
#echo="echo "
unset echo
DS="test_ds"
PROJECT="$(gcloud config get project 2>/dev/null)"
echo > logs/test.log

bq mk ${DS}  


create_test_data() {
  test_file=${1:-types_to_test.json}
  [[ -f $test_file ]] || return 1
  for pair in $(jq -r '.[]|"\(.pb),\(.bq)"' $test_file ) 
  do 
    a=(${pair//,/ }) ;  msg_s=${a[0]} ; bq_s=${a[1]}
    if [[ ! $msg_s == "null" ]]
    then   
      #echo "creating data for : $pair"
      create_schema_file pb test $msg_s $bq_s
    else
      echo "Pair is not protocol buffer"
    fi
  done
}

create_schema_file() {
 [[ $1 = "asvc" ]] && jq '.fields|=[{"name":"$2"},{"type":"$3"}]' templates/avro >schemas/$2_$3_$4.$1
 [[ $1 = "pb" ]] &&  export MAP="$3 $2 = 1;"; cat templates/pb | envsubst >schemas/$2_$3_$4.$1
 echo "[ { \"name\": \"$2\", \"type\": \"$4\", \"mode\": \"NULLABLE\" } ]" > schemas/$2_$3_$4_table.json
}

test_schema() {
  msg_schema=$1
  noext=${msg_schema//.*}; name=${noext//*\/} 
  tbl_schema=${noext}_table.json
  tbl="${PROJCET}:${DS}.${name}"
  $echo gcloud pubsub schemas create $name --definition-file=$msg_schema --type protocol-buffer  $LOGREPLACE  
  $echo gcloud pubsub topics create $name --schema $name --message-encoding json $LOGREPLACE  
  $echo bq mk -t --schema $tbl_schema "${tbl}" $LOGREPLACE  
  $echo bq set-iam-policy  "${tbl}" templates/policy.json  $LOGREPLACE  
  $echo gcloud pubsub subscriptions create $name --bigquery-table="${tbl}" --topic $name  --use-topic-schema  
}



cleanup() {
  gcloud pubsub topics list --format json | jq -r '.[].name' | xargs -n1 gcloud pubsub topics delete  $LOGREPLACE  
  gcloud pubsub subscriptions list --format json | jq -r '.[].name' | xargs -n1 gcloud pubsub subscriptions delete   $LOGREPLACE  
  gcloud pubsub schemas list --format json | jq -r '.[].name' | xargs -n1 gcloud pubsub schemas delete --quiet  $LOGREPLACE  
  echo Y | bq rm -r test_ds  $LOGREPLACE  
}

run_tests() {
  echo "creating test data" && create_test_data $1
  for test in $(ls schemas/*.pb)
  do
    echo "test schema: $test"
    test_schema $test
  done
}

