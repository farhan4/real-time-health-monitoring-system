# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import expr, from_json, col, concat
from pyspark.sql import Window
# import msgpack
import json
from datetime import datetime
from pyspark.sql import functions as F


# Defining the patient vital info data Schema
valueschema = StructType([
	
	StructField("customerId", 	IntegerType(), 		True),
	StructField("heartBeat", 	IntegerType(), 		True),
	StructField("bp", 			IntegerType(), 	    True)

])

	
# Creating spark session
spark = SparkSession  \
	.builder  \
	.appName("StructuredSocketRead")  \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')


# .option("failOnDataLoss", False) \
# .option("startingOffsets", "earliest") \
# .option("auto.offset.reset", "latest") \
# .option("maxOffsetsPerTrigger",10)


# Reading event stream from Kafka 
# lines = spark  \
# 	.readStream  \
# 	.format("kafka")  \
# 	.option("kafka.bootstrap.servers","54.197.249.158:9092")  \
# 	.option("subscribe","patients_vital_info")  \
# 	.option("failOnDataLoss", False) \
# 	.option("startingOffsets", "earliest") \
# 	.load()


# Reading event stream from Kafka 
lines = spark  \
	.readStream  \
	.format("kafka")  \
	.option("kafka.bootstrap.servers","54.197.249.158:9092")  \
	.option("subscribe","patients_vital_info")  \
	.option("failOnDataLoss", False) \
	.option("auto.offset.reset", "earliest") \
	.load()



# Reading the Key-value pair data
kafkaDF = lines.selectExpr("cast(value as string)")


# Converting dataframe from json format with one column data
kafkaDF = kafkaDF.select(from_json(col("value"), valueschema).alias("data"))


# Adding column with different fields
kafkaDF = kafkaDF.withColumn("customerId", 	kafkaDF["data"]["customerId"]) \
				 .withColumn("heartBeat", 	kafkaDF["data"]["heartBeat"]) \
				 .withColumn("bp", 			kafkaDF["data"]["bp"]) \
				 .drop("data")


# Adding column with message time field
kafkaDF = kafkaDF.withColumn("message_time", F.current_timestamp())


# .trigger(continuous = '1 second') \

# query = kafkaDF \
# 	.writeStream \
# 	.outputMode("append") \
# 	.format("console") \
# 	.start()


# Writing data to HDFS in parquet file format
query2 = kafkaDF \
		.writeStream \
		.format("parquet") \
		.outputMode("append") \
		.option("truncate", "false") \
    	.option("path", "patient_vitals") \
    	.option("checkpointLocation", "patient_vitals_chk") \
    	.trigger(processingTime = "10 seconds") \
    	.option("numRows", 10)  \
    	.start()


query2.awaitTermination()

