from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    print("Printing Schema of orders_df1: ")
    orders_df1.printSchema()
    
    # Write final result into console for debugging purpose
        windowedCounts = orders_df1 \
        .groupBy(window(orders_df1.timestamp, "10 seconds", "5 seconds")) \
        .agg(count("*").alias("count")) \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("complete") \
        .option("truncate", "false") \
        .format("console")

    # Write final result into console for debugging purpose
    agg_write_stream = windowedCounts.start()

    agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")
