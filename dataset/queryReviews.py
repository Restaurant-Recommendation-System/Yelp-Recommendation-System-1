from __future__ import print_function  # Python 2/3 compatibility
import boto3

dynamodb = boto3.resource('dynamodb')

table = dynamodb.Table('review')

items = table.scan()
print(items['Count'])