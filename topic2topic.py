#!/usr/bin/env python
from unittest import expectedFailure
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1 import types
from google.pubsub_v1.types import Schema, Encoding

from config import *

from google.api_core.exceptions import NotFound, AlreadyExists
from concurrent.futures import TimeoutError
from google.cloud.pubsub_v1 import SchemaServiceClient

debug = False
PUBLISH_BATCH_SIZE = 10
TIMEOUT = 100
# create a class that serves as a "broker" - subscribe to one topic and publish to another

print (f"creating topics and subscriptions in {PROJECT}")
    
def create_schema(file="schema/taxi_rides.pb",id=None,project=PROJECT,type=None):

    if id is None: id = file.split(".")[0].split("/")[-1]
    if type is None: type=file.split(".")[1]
    if type == "pb": 
         type = Schema.Type.PROTOCOL_BUFFER
    elif type == "asvc":
         type = Schema.Type.AVRO
    else: 
        print("unknown type in cheate schema")
        return None

    with open(file, "rb") as f:
        schema_source = f.read().decode("utf-8")

    schema_client = SchemaServiceClient()
    schema_path = schema_client.schema_path(project, id)
    schema = Schema(name=schema_path, type_=type, definition=schema_source)

    try:
        result = schema_client.create_schema(
            request={"parent": f"projects/{project}", "schema": schema, "schema_id": id}
        )
        print(f"Created a schema using an Avro schema file:\n{result}")
    except AlreadyExists:
        print(f"Schema: {id} already exists.")
    except Exception as e:
        print (f"Eeception when creating schema ({id}) ({e})")
    
    return schema

# Creating or replace the new topic
def create_topic(project=PROJECT,topic=TOPIC,replace=False,schema=None,encoding=Encoding.JSON):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)
    try :
        if replace: publisher.delete_topic({"topic": topic_path})
        return publisher.get_topic(request={"topic": topic_path})
    except Exception as e:
        if isinstance(e,NotFound):
            print ("Topic does not exist, creating it")
            request={"name": topic_path}

            if schema is not None:
                request["schema_settings"] = {"schema": schema, "encoding": encoding}

            return publisher.create_topic(request=request)
        else:
            print(f"exception when creating topic ({e}) error type ({type(e)}) ")
            return 


def create_subscription(project=PROJECT,src_project=SRC_PROJECT,topic=TOPIC,subscription=SUBSCRIPTION,replace=False):
    # Creating or replace a subscription from source topic to local subscription
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(src_project ,topic)
    subscription_path = subscriber.subscription_path( project,subscription)

    with subscriber:
        try:
            if replace: subscriber.delete_subscription({"subscription": subscription_path})
            return subscriber.get_subscription({"subscription": subscription_path})
        except NotFound:
            print (f"Subscription does not exist - creating new subscription")
            request={"name": subscription_path, "topic": topic_path}
            return subscriber.create_subscription(request)
        except Exception as e:
            print(f"exception when creating subscription ({e}) error type ({type(e)}) ")
            return 

def subscribe(subscription_path):
    subscriber = pubsub_v1.SubscriberClient()
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=pull_callback)

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            streaming_pull_future.result(timeout=TIMEOUT)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
        
def pull_callback(msg: pubsub_v1.subscriber.message.Message) -> None:
    #print(f".",end=".") #Received {msg.data}.")
    if debug:
        f.write(msg.data)
        print({msg.data})
    publish(msg,PUBLISH_BATCH_SIZE)
  
    msg.ack()
    
def publish(msg,publish_batch_size=500,project=PROJECT,topic=TOPIC):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)
    publisher.batch_settings = types.BatchSettings(max_messages=publish_batch_size)
    response = publisher.publish(topic_path, msg.data)
   

# Main
print (f"Source topic: '{TOPIC}' in '{SRC_PROJECT}' PROJCET")
with open ("logs/records.json",'wb') as f:
    schema = create_schema("schemas/taxi_rides.pb")
    print (f"Schema to be used on the topic: {schema.name}")
    topic = create_topic(schema=schema.name,encoding=Encoding.JSON)
    print(f"New Tpoic: {topic}")
    subscription = create_subscription()
    print(f"Subscription: {subscription.name} listnening for messages in topic: {subscription.topic}")
    subscribe(subscription.name)


