import logging
import boto3
from botocore.exceptions import ClientError
import json
import os
import pandas as pd

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


def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to a S3 bucket.
    """
    try:
        if object_name is None:
            object_name = os.path.basename(file_name)
        response = s3_client.upload_file(
            file_name, bucket, object_name)
    except ClientError:
        logger.exception('Could not upload file to S3 bucket.')
        raise
    else:
        return response


def transform_file(file_name,path):
    df = pd.read_csv(file_name)
    new_df = df.groupby('departure_name').describe()
    new_df.to_csv(f'{file_name}_transformed')
    new_file = os.path.join(path, f'{file_name}_transformed')
    #table1 = df.groupBy("departure_name").count().withColumnRenamed("count", "count_taken")
    #table2 = df.groupBy("return_name").count().withColumnRenamed("count", "count_returned")
    #result_table = table1.join(table2, table1["departure_name"] == table2["return_name"]).select('departure_name','count_taken','count_returned')
    object_name = f'{file_name[7:]}_transformed'
    bucket = 'my-bucket'
    upload_file(new_file, bucket, object_name)
    logger.info('Files uploaded to S3 bucket successfully.')


def open_file():
    """
    for loop for csv files in the folder
    """
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        object_name = f'{file[7:]}'
        bucket = 'my-bucket'
        logger.info('Uploading file to S3 bucket in LocalStack...')
        upload_file(file_name, bucket, object_name)
        #logger.info('Transforming and uploading file to S3 bucket in LocalStack...')
        #transform_file(file_name, path)
        #logger.info('Files uploaded to S3 bucket successfully.')


if __name__ == '__main__':
    open_file()
