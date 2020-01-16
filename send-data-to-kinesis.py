import boto3
import json
import random
import time

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

my_stream_name = "kinesis-lambda-sqs-tutorial-stream"

def put_to_stream(word, partition):
    payload = {
                'word': word,
                'partition': partition
              }

    print(payload)

    put_response = kinesis_client.put_record(
                        StreamName=my_stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=partition)

    print(f"response sending data to kinesis: {put_response}")

while True:
    index = random.randint(0, 1)
    despacito = "despacito"
    partition_key = ["shardCero", "shard_oneeee"]
    # espanol is send to shard 0, english to shard 1

    put_to_stream(despacito, partition_key[index])

    # wait for 5 second
    time.sleep(1)