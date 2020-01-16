import boto3
import json
from datetime import datetime
import time

my_stream_name = "kinesis-lambda-sqs-tutorial-stream"

kinesis_client = boto3.client('kinesis', region_name='us-east-1')
response = kinesis_client.describe_stream(StreamName=my_stream_name)

my_shard_id_0 = response['StreamDescription']['Shards'][0]['ShardId']
my_shard_id_1 = response['StreamDescription']['Shards'][1]['ShardId']

print(f"my_shard_id_0: {my_shard_id_0}")
print(f"my_shard_id_1: {my_shard_id_1}")


shard_iterator_0 = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id_0,
                                                      ShardIteratorType='TRIM_HORIZON')
shard_iterator_1 = kinesis_client.get_shard_iterator(StreamName=my_stream_name,
                                                      ShardId=my_shard_id_1,
                                                      ShardIteratorType='TRIM_HORIZON')

my_shard_iterator_0 = shard_iterator_0['ShardIterator']
my_shard_iterator_1 = shard_iterator_1['ShardIterator']

print(f"my_shard_iterator_0: {my_shard_iterator_0}")
print(f"my_shard_iterator_1: {my_shard_iterator_1}")


record_response_0 = kinesis_client.get_records(ShardIterator=my_shard_iterator_0,
                                              Limit=2)
record_response_1 = kinesis_client.get_records(ShardIterator=my_shard_iterator_1,
                                              Limit=2)
print(f"record_response_0: {record_response_0}")
print(f"record_response_1: {record_response_1}")

while 'NextShardIterator' in record_response_0 or "NextShardIterator" in record_response_1:
	record_response_0 = kinesis_client.get_records(ShardIterator=record_response_0['NextShardIterator'], Limit=20)
	record_response_1 = kinesis_client.get_records(ShardIterator=record_response_1['NextShardIterator'], Limit=20)
	print(f"record_response_0: {record_response_0}")
	print(f"record_response_1: {record_response_1}")

	# wait for 5 seconds
	time.sleep(1)