# short demo to show bigquery subscription
- set the project and other naming if you like in config.py
- run topic2topic.py  in the background `./topic2topic.py & ` this will create a local topic with a protocol buffer schema, and subscribe to public taxi rides topic (pulling from the subscriptino and publishing to the local topic)
- `source pubsub_bq.sh.sh` and `run start_pubsub_bq` this will create a dataset, a table with the schema, grant pubsub access to the table and create a big query subscription to the new topic
- `cleanup` stop topic2topic and remove the dataset and all topics/subscriptions/schemas in the project matching "*taxi*"! (so be careful not to loose stuff if your project has other topics/subscriptions).
- `query_bq` to list events in bq table 
* Note:  doubles & floats in msg schemas do not work at this time, so for the sake of this small demo, I've removed the fields from the bq table and ignored missing fields!
  