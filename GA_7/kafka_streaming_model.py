from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.regression import LinearRegression,LinearRegressionModel
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import VectorAssembler
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

    temp_path = "./models/test0"
    model_path = temp_path + "/lr_model"
    model2 = LinearRegressionModel.load(model_path)
    
    test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    df_m = model2.transform(test0).take(1)
    dataframe_1 = spark.createDataFrame(df_m)


    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")
    
    # Define a schema for the orders data
    orders_schema = StructType() \
        .add("id", StringType()) \
        .add("msg", FloatType()) 

    print("Printing Schema of orders_df1: ")
    orders_df1.printSchema()
    #orders_df1.show()
    dataframe_1.show()
    
    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    df_6 = orders_df3.select('msg')
    df_6.printSchema()
    assembler = VectorAssembler(inputCols=['msg'],
                outputCol="features")
    output = assembler.transform(df_6)
    out_df = model2.transform(output.select('features'))#.take(1)
    out_df.printSchema()

    #test0 = spark.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    #df_5 = model2.transform(output).take(1)
    #print(df_5)

    orders_df4 = orders_df3.groupBy("id") \
        .agg({'id': 'count'}) \
        .select("id", col("count(id)") \
        .alias("total_line_count"))

    print("Printing Schema of orders_df3: ")
    orders_df3.printSchema()

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()

    # Write final result into console for debugging purpose
    windowedCounts = out_df \
        .writeStream \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console")

    # Write final result into console for debugging purpose
    agg_write_stream = windowedCounts.start()

    agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")
