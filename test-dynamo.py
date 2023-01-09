import json
import pandas as pd
import logging
from datetime import date, datetime
import os
from decimal import Decimal
import boto3
from botocore.exceptions import ClientError

AWS_REGION = 'us-east-1'
AWS_PROFILE = 'localstack'
ENDPOINT_URL = 'http://localhost:4566'


# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

boto3.setup_default_session(profile_name=AWS_PROFILE)

dynamodb_client = boto3.client(
    "dynamodb", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)


def json_datetime_serializer(obj):
    """
    Helper method to serialize datetime fields
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def create_dynamodb_table(table_name):
    """
    Creates a DynamoDB table.
    """
    try:
        response = dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    "AttributeName": "id",
                    "KeyType": "HASH",
                },
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "id",
                    "AttributeType": 'N',
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1
            })

    except ClientError:
        logger.exception('Could not create the table.')
        raise
    else:
        return response



def open_file():
    """
    for loop for csv files in the folder
    """
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        #table_name = f'{file[7:]}'
        table_name = 'raw_data'
        create_dynamodb_table(table_name)
        #add_dynamodb_table_item(table_name, file_name)


if __name__ == '__main__':
    create_dynamodb_table('raw_data')