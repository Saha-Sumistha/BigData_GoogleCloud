from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import requests

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    response = requests.get('https://storage.googleapis.com/mukesh-iitm/file_ga7.txt')
    response_text=[response.text.split("\n")][0]
    batchsize =10
    message_list = []
    message = None
    for i in range(0,100,10):
        batch_msg = response_text[i:batchsize]
        batchsize +=10
        message = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        message["id"] = 1
        message["msg"] =batch_msg 
        message["no_of_line"] =len(batch_msg) 
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(10)

    # print(message_list)

   print("Kafka Producer Application Completed. ")
