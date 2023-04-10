
# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import  StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType, TimestampType
from pyspark.sql.functions import expr, from_json, col, concat
from pyspark.sql import Window
import json
from pyspark.sql import HiveContext
from os.path import abspath
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
import happybase
from pyspark.sql.functions import to_json, struct

# Defining the schema
valueschema = StructType([
	
	StructField("customerId", 	IntegerType(), 		True),
	StructField("heartBeat", 	IntegerType(), 		True),
	StructField("bp", 			IntegerType(), 	    True),
	StructField("message_time", TimestampType(), 	True),
	

])



# warehouse_location = 'hdfs://localhost:8020/user/hive/warehouse'
# warehouse_location = abspath('hdfs://localhost:8020/user/hive/warehouse')
# warehouse_location = '/user/hive/warehouse'
# warehouse_location = abspath('/user/hive/warehouse/')
warehouse_location = 'hdfs:///user/hive/warehouse'
# warehouse_location = abspath('hdfs:///user/hive/warehouse')



# .config("spark.sql.warehouse.dir", warehouse_location) \
# .enableHiveSupport() \


# Creating spark session
spark = SparkSession  \
	.builder  \
	.appName("HiveRead")  \
	.getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


## Reading stream of patients vital info
patient_vital_df = spark.readStream \
	.schema(valueschema) \
	.parquet("patient_vitals/part-*.snappy.parquet")




### READING PATIENT CONTACT INFO FROM HIVE
spark.conf.set("spark.sql.hive.convertMetastoreParquet", False)
sqlContext = HiveContext(spark.sparkContext)

databases = sqlContext.sql('show databases') 
databases.show()

dbselect = sqlContext.sql('use health_care') 

tables = sqlContext.sql('show tables') 
tables.show()


# patient_vital_df = sqlContext.sql('select * from health_care.patients_vital_info where message_time is not null order by message_time desc') 
# patient_vital_df.cache()
# patient_vital_df.show(5)

# READING PATIENT CONTACT INFO
patient_contact_df = sqlContext.sql('select * from health_care.patients_contact_info') 
patient_contact_df.cache()
patient_contact_df.show(5)



# INNER JOIN TOTAL INFO
patient_total_info = patient_vital_df.join(patient_contact_df , col("customerid") == col("health_care.patients_contact_info.patientid"), "inner")
patient_total_info = patient_total_info.withColumn("alert_generated_time", F.current_timestamp())
patient_total_info = patient_total_info.withColumn("to_be_alerted", lit(0))
patient_total_info = patient_total_info.withColumn("alert_message", lit(" "))

spark.catalog.clearCache()


print("connecting to HBase")
con = happybase.Connection('localhost')

con.open()
print("Connected")

print("Listing tables...")
print(con.tables())


table = con.table('threshold_hive')

# Scanning each row  of the table
for key, data in table.scan():

	attribute = str(data[b'attr:attribute'].decode("utf-8"))
	low_age_limit = int(data[b'limit:low_age_limit'].decode("utf-8"))
	high_age_limit = int(data[b'limit:high_age_limit'].decode("utf-8"))
	low_value = int(data[b'limit:low_value'].decode("utf-8"))
	high_value = int(data[b'limit:high_value'].decode("utf-8"))
	alert_flag = int(data[b'msg:alert_flag'].decode("utf-8"))
	alert_message = str(data[b'msg:alert_message'].decode("utf-8"))



	if attribute == "bP" and alert_flag == 1:
		patient_total_info = patient_total_info.withColumn("to_be_alerted",  when( (col("age") >= low_age_limit) & (col("age") <= high_age_limit) & (col("bp") >= low_value) & (col("bp") <= high_value), lit(1) ) \
																		.otherwise(col("to_be_alerted"))
			)
	
	
	if attribute == "heartBeat" and alert_flag == 1:
		patient_total_info = patient_total_info.withColumn("to_be_alerted",  when( (col("age") >= low_age_limit) & (col("age") <= high_age_limit) & (col("heartbeat") >= low_value) & (col("heartbeat") <= high_value), lit(1) ) \
																		.otherwise(col("to_be_alerted"))
			)
	

	patient_total_info = patient_total_info.withColumn("alert_message", when( (col("to_be_alerted") == 1) & (col("alert_message") == " ") , alert_message) \
																		.otherwise(col("alert_message"))  
			)


patient_total_info = patient_total_info.filter(patient_total_info["to_be_alerted"] == 1)

patient_total_info = patient_total_info.select(

		patient_total_info["bp"].alias("bp"),
		patient_total_info["heartbeat"].alias("heartbeat"),
		patient_total_info["patientname"].alias("patientname"),
		patient_total_info["patientaddress"].alias("patientaddress"),
		patient_total_info["phone_number"].alias("phone_number"),
		patient_total_info["age"].alias("age"),
		patient_total_info["admitted_ward"].alias("admitted_ward"),
		patient_total_info["message_time"].alias("input_message_time"),
		patient_total_info["alert_generated_time"].alias("alert_generated_time"),
		patient_total_info["alert_message"].alias("alert_message")

	)




# query = patient_total_info.selectExpr( "to_json(struct(*)) AS value") \
# 	.writeStream \
# 	.outputMode("append") \
# 	.format("console") \
# 	.option("truncate", False) \
# 	.start()

# Writing anomalies to the Kafka  
query = patient_total_info.selectExpr( "to_json(struct(*)) AS value") \
			.writeStream \
			.format("kafka") \
			.outputMode("append") \
			.option("kafka.bootstrap.servers", "54.197.249.158:9092") \
			.option("topic", "doctor") \
			.option("checkpointLocation", "alerts") \
			.start()


query.awaitTermination()









