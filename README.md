# real-time-health-monitoring-system

A Real Time Health Monitoring System was created. The project involved the following steps - 

1. Producing data to the kafka topic reading it from a MySQL database.
2. The data was read in stream format from kafka topic and message_time timestamp field was added and was saved into HDFS in parquet file format.
3. The patient vital info data was also saved into the Hive table.
4. The threshold data which was a reference data was saved in to the Hbase using Hbase-Hive integration.
5. The patient contact data was fetched using SQOOP from RDS and saved into hive table.
6. The spark streaming job was created to compare the patient vital info with the threshold data and the anomalies was pushed into the Kafka queue.
7. The anomalies was mailed to the patient using AWS SNS service.
