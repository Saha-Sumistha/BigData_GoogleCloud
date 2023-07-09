from google.cloud import pubsub_v1
from google.cloud import storage

def callback(message):
    output=str(message.data,"utf-8")
    print(f"Pull file name: {output}.")
    if output!='My first message!':
      storage_client=storage.Client()
      bucket=storage_client.bucket('test-2204')
      blob=bucket.blob(output)
      cnt=0
      with blob.open("r") as f:
        x=f.readlines()
        print(x)
        cnt=len(x)
      print('Total Number of lines: '+str(cnt))

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")
    file_name=file['name']
    file_name_byte=bytes(file_name,'utf-8')
    publisher=pubsub_v1.PublisherClient()
    topic_name='projects/seraphic-disk-383014/topics/testga6'
    future = publisher.publish(topic_name, file_name_byte)
    future.result()
    subscription_name = 'projects/seraphic-disk-383014/subscriptions/iitga6'
    subs = pubsub_v1.SubscriberClient()
    future_sub = subs.subscribe(subscription_name, callback)