from kafka import KafkaConsumer
from json import loads
import time

KAFKA_CONSUMER_GROUP_NAME_CONS = "test-consumer-group"
KAFKA_TOPIC_NAME_CONS = "test-topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":

    print("Kafka Consumer Application Started ... ")
    try:
        # auto_offset_reset='latest'
        # auto_offset_reset='earliest'
        consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME_CONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
        value_deserializer=lambda x: loads(x.decode('utf-8')))

        for message in consumer:
            #print(dir(message))
            #print(type(message))
            print("Key: ", message.key)
            message = message.value
            print()
            print("=============================")
            print("New Message Received: ")
            print("=============================")
            print()
            #print("Message received: ", message)
            print("ID: ", message["id"])
            print("Message: ", message["msg"])
            print("No of Lines: ", message["no_of_line"])
            #time.sleep(5)
    except Exception as ex:
        print("Failed to read kafka message.")
        print(ex)
