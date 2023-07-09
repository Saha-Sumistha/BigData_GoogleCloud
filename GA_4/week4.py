#imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, when, isnan, isnull, col, lit
from pyspark.sql.types import StringType

##Creating spark session
spark= SparkSession.builder.appName("ga4").getOrCreate()

#reading Customer Master information csv from GCP Bucket with spark command
customer_master_data = spark.read.csv("gs://13042023bucket/Customer Master Dataframe - Information.csv", header=True, inferSchema= True )
#reading Customer Master update csv from GCP Bucket with spark command
customer_master_updates = spark.read.csv("gs://13042023bucket/Customer Master Dataframe - Updates.csv", header=True, inferSchema=True)

customer_master_data.show()
customer_master_updates.show()

###As there is no command to update directly we will do the below steps###

#joining both csv on Name column
updated = customer_master_updates.join(customer_master_data, on ="Name")
updated.show()

#dropping DOB column 
updated = updated.drop("DOB")
updated = updated.withColumnRenamed('updated_DOB','DOB')

updated = updated.withColumn("validity_start", lit(current_date()))

new_record = customer_master_updates.join(customer_master_data, on = "Name", how="right_outer")

new_record.show()

null_count = new_record.filter(col("updated_DOB").isNull()).count()


new_record = new_record.withColumn('validity_end', when(isnull(col('updated_DOB')), col('validity_end')))
new_record = new_record.drop("updated_DOB")
new_record = new_record.withColumn("validity_end", when(new_record.validity_end.isNull(), lit(current_date())).otherwise(new_record.validity_end))
new_record.show()
updated.show()

type2 = new_record.unionByName(updated)
type2.show()

type2.write.format("csv").option("header", True).mode("overwrite").save("gs://13042023bucket/output.csv")