

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
import boto3




producer = None
consumer = None
admin_client = None
topic_name = 'doctor'
client = boto3.client('sns', region_name = 'us-east-1', aws_access_key_id="AKIAYQR5HNKQK7OITP67",aws_secret_access_key="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")


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




# Send Alert to Consumer
def send_alert(message):

	try:
		
		global client

		arn = 'arn:aws:sns:us-east-1:451401607342:Capstone_SNS:dffe9c3a-3f3f-447f-ba7f-6f5269a10788'

		response = client.publish(
			TargetArn = arn,
			Message = json.dumps(message)
		)

		print(response)
	

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
					send_alert(m.value)
					
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


def run_all():
	list_messages()


if __name__ == "__main__":
	run_all()

		