from __future__ import print_function  # Python 2/3 compatibility
import boto3
import json

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('review')
with open("../input/review.json") as json_file:
    with table.batch_writer() as batch:
        for line in json_file:
            review = json.loads(line)
            batch.put_item(Item=review)
    print("Done!")