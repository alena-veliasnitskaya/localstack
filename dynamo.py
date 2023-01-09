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

s3_client = boto3.client("s3", region_name=AWS_REGION,
                         endpoint_url=ENDPOINT_URL)

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
                    'AttributeName': 'departure',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'departure',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            })

    except ClientError:
        logger.exception('Could not create the table.')
        raise
    else:
        return response


def add_dynamodb_table_item(table_name, file_name):
    data = json.loads(pd.read_csv(file_name).to_json(orient='records'), parse_float=Decimal)
    my_dict = {'item': data, 'table': table_name}
    dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
    dynamotable = dynamodb_resource.Table(table_name)
    for record in my_dict['item']:
        dynamotable.put_item(Item=record)


def transform_pandas(table_name):
    if len(table_name) <= 8:
        df = pd.read_csv(s3_client.get_object(Bucket='my-bucket', Key=table_name).get("Body"))
        df['departure'] = df['departure'].astype('datetime64[ns]')
        df['month_day'] = df['departure'].dt.to_period('D')
        df['avg_distance_by_month'] = df['distance (m)'].mean()
        df['avg_distance_by_day'] = df.groupby('month_day')['distance (m)'].transform('mean')
        df['avg_duration_by_month'] = df['duration (sec.)'].mean()
        df['avg_duration_by_day'] = df.groupby('month_day')['duration (sec.)'].transform('mean')
        df['avg_speed_by_month'] = df['avg_speed (km/h)'].mean()
        df['avg_speed_by_day'] = df.groupby('month_day')['avg_speed (km/h)'].transform('mean')
        df['avg_temperature_by_month'] = df['Air temperature (degC)'].mean()
        df['avg_temperature_by_day'] = df.groupby('month_day')['Air temperature (degC)'].transform('mean')
        new_df = df.filter(['avg_distance_by_month', 'avg_distance_by_day', 'avg_duration_by_month',
                            'avg_duration_by_day', 'avg_speed_by_month', 'avg_speed_by_day',
                            'avg_temperature_by_month', 'avg_temperature_by_day'], axis=1)
        data = json.loads(df.to_json(orient="records"), parse_float=Decimal)
        my_dict = {'item': data, 'table': table_name}
        dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION, endpoint_url=ENDPOINT_URL)
        dynamotable = dynamodb_resource.Table(table_name)
        for record in my_dict['item']:
            dynamotable.put_item(Item=record)


def open_file():
    """
    for loop for csv files in the folder
    """
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        table_name = f'{file[7:]}'
        #create_dynamodb_table(table_name)
        #add_dynamodb_table_item(table_name, file_name)
        transform_pandas(table_name)


if __name__ == '__main__':
    open_file()