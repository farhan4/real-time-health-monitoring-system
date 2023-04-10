
import logging
import pymysql as SQ
import msgpack
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka import TopicPartition
from kafka import OffsetAndMetadata
from kafka import KafkaAdminClient
from kafka.admin import KafkaAdminClient, NewTopic
import json
import binascii
import time

producer = None
consumer = None
admin_client = None
topic_name = 'patients_vital_info'


# Connecting to Kafka Producer
def connect_kafka_producer():
	
	try:
		global producer

		producer = KafkaProducer(

			bootstrap_servers=['localhost:9092'],
			# value_serializer = msgpack.dumps,
			# value_serializer = bytearray(),
			value_serializer = json.dumps,
			# compression_type = 'lz4', 
			api_version = (2, 5, 0) ,
			# max_request_size = 10,
			# buffer_memory = 10
		)

		return producer

	except Exception as e:
		print(e)

# Connecting to Admin Client
def connect_admin_client():

	try:

		global admin_client

		admin_client = KafkaAdminClient(
			bootstrap_servers = "localhost:9092",
			api_version = (2, 5, 0) 	
		)

		return admin_client

	except Exception as e:
		print(e)

# Creating Topic with given number of partitions and replication factor
def create_topic(topic_name, number_partitions):

	try:
		
		global admin_client

		if admin_client == None:
			admin_client = connect_admin_client()

		topic_list = []
		topic_list.append(NewTopic(name = topic_name, num_partitions = number_partitions , replication_factor = 1))
		admin_client.create_topics(new_topics = topic_list, validate_only = False, timeout_ms = 5000)
	
	except Exception as e:
		print(e)


# Connecting to Kafka Consumer
def connect_kafka_consumer(ugroup_id):
	
	
	try:

		global consumer

		consumer = KafkaConsumer(
			# value_deserializer = msgpack.unpackb, 
			value_deserializer = json.loads,
			group_id = ugroup_id , 
			api_version = (2, 5, 0) , 
			enable_auto_commit = False , 
			bootstrap_servers=['localhost:9092']
		)

		return consumer
	
	except Exception as e:
		print(e)

# Sending Data to Kafka Producer
def send_data(data, partition):

	try:

		global producer

		if producer == None:
			producer = connect_kafka_producer()


		producer.send(topic_name, partition = partition, value = data)
		# producer.flush()

	except Exception as e:
		print(e)
	
# Reading from MySQL database and pushing to kafka queue
def read_mysql_data():

	try:

		db = SQ.connect(host = 'upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com', user = 'student', password = 'STUDENT123', db = 'testdatabase')
		cur = db.cursor()

		cur.execute("select * from  testdatabase.patients_vital_info")

		result = cur.fetchall()

		for r in result:
			
			data = {}
			
			customerId = r[0]
			heartBeat = r[1]
			bp = r[2]
			
			data.update(
				{
					"customerId" 	: customerId,
					"heartBeat" 	: heartBeat,
					"bp" 			: bp
				}
			)

			send_data(data, 0)

			time.sleep(1)


		cur.close()
		db.close()	

	
	except Exception as e:
		print(e)
	

# Running all the functions
def run_all():

	try:
		
		global topic_name

		create_topic(topic_name, 1)
		read_mysql_data()
		list_messages()
	
	except Exception as e:
		print(e)


# Listing of the messages
def list_messages():

	try:

		global consumer, topic_name

		group_id = 'list-group'
		partition = 0

		if consumer == None:
			consumer = connect_kafka_consumer(group_id)

		
		topic_partition = TopicPartition(topic= topic_name, partition= partition)
				
		try:
			consumer.assign([topic_partition])
		except Exception as e:
			print(e)


		consumer.seek_to_beginning(topic_partition)

		message = consumer.poll(timeout_ms = 1000)

		flag = True

		offsets = {}
		offsets[topic_partition] = OffsetAndMetadata(0, '')
		offset_value = 0


		while flag:	

			msgs = []

			if topic_partition in message:
				msgs = message[topic_partition]

			if len(msgs) > 0:
				
				for m in msgs:
					print(m.value)
					
				try:
					offset_value = offset_value + len(msgs) - 1 
					offsets[topic_partition] = OffsetAndMetadata(offset_value, '')
					consumer.commit(offsets)
				except Exception as e:
					print(e)
			else:
				flag = False		


			message = consumer.poll(timeout_ms = 1000)

			if flag == False:
				break	

		
	except Exception as e:
		print(e)


if __name__ == "__main__":
	run_all()
	

