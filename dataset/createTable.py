import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb')

# ----- Review Table ----

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='review',
    KeySchema=[
        {
            'AttributeName': 'review_id',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'stars',
            'KeyType': 'RANGE'  # Sort key
        },
    ],
    LocalSecondaryIndexes=[
        {
            'IndexName': 'UserByDate',
            'KeySchema': [
                {
                    'AttributeName': 'review_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'user_id',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'BusinessByDate',
            'KeySchema': [
                {
                    'AttributeName': 'review_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'business_id',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'review_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'user_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'business_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'stars',
            'AttributeType': 'N'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='review')

# Print out some data about the table.
print(table.item_count)
