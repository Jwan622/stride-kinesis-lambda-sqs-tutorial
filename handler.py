import logging
import os
import boto3
import base64
import json


logger = logging.getLogger()
logger.setLevel(logging.INFO)
# boto3 comes auto installed with AWS
sqs = boto3.client('sqs')

queue_url_english = 'kinesis-lambda-sqs-tutorial-english'


def readEvents(event, context):
	# The event from kinesis looks like this:
	# event: {'Records': [{'kinesis': {'kinesisSchemaVersion': '1.0',
	# 									'partitionKey': 'shard1',
	# 								 	'sequenceNumber': '49603189052963443070854894544155161202176885066678927378',
	# 								 	'data': 'eyJ3b3JkIjogImVuZ2xpc2giLCAicGFydGl0aW9uIjogInNoYXJkMSJ9',
	# 								 	'approximateArrivalTimestamp': 1578804638.945},
	# 									'eventSource': 'aws:kinesis',
	# 					 'eventVersion': '1.0',
	# 					 'eventID': 'shardId-000000000001:49603189052963443070854894544155161202176885066678927378',
	# 					 'eventName': 'aws:kinesis:record',
	# 					 'invokeIdentityArn': 'arn:aws:iam::516088479088:role/kinesis-lambda-sqs-tutorial-dev-us-east-1-lambdaRole',
	# 					 'awsRegion': 'us-east-1',
	# 					 'eventSourceARN': 'arn:aws:kinesis:us-east-1:516088479088:stream/kinesis-lambda-sqs-tutorial-stream'}]}
	logging.info(f"event: {event}")
	for record in event['Records']:
		logging.info("lambda: kinesis-lambda-sqs is reached")
		payload = base64.b64decode(record["kinesis"]["data"])
		data = json.loads(payload)
		logger.info("Decoded payload: " + str(data))
		logger.info(f"context: {context}")
		logger.info(f"env variables: {os.environ}")

		# fake api call to google translate
		translated = fake_api_call_to_google_translate(data["word"])

		# Send message to SQS queue
		sqs_url = sqs.get_queue_url(QueueName=queue_url_english)
		logger.info(f"sending message to sqs...{translated}")
		response = sqs.send_message(
			QueueUrl=sqs_url['QueueUrl'],
			MessageBody=(
				json.dumps({
					"original": data["word"],
					"translated": translated,
					"source": "lambda"
				})
			)
		)

		logger.info(f"sqs response: {response['MessageId']}")

		return response

def fake_api_call_to_google_translate(data):
	logger.info(f"making api call to google translate for word: {data}")
	return "Slowly"