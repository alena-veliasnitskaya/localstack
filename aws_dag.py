import datetime
import os
from datetime import timedelta
import json
import logging
import boto3

from airflow.hooks.filesystem import FSHook
from airflow.utils.context import Context
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.connection import Connection
from airflow.exceptions import AirflowFailException
from botocore.exceptions import ClientError

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, TimestampType

args = {
    'owner': 'alena',
    'start_date': days_ago(1),
    'depends_on_past': False,
}
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


def open_file():
    """
    for loop for csv files in the folder
    """
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        object_name = f'{file_name}'
        bucket = 'my-bucket'
        logger.info('Uploading file to S3 bucket in LocalStack...')
        upload_file(file_name, bucket, object_name)
        logger.info('Transforming and uploading file to S3 bucket in LocalStack...')



def open_transform_file():
    path = '/home/alena/Data/Localstack/test_data'
    files = os.listdir(path)
    for file in files:
        file_name = os.path.join(path, file)
        """transforming files using pyspark"""
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName('PySpark_Tutorial') \
            .getOrCreate()
        my_schema = StructType() \
            .add("departure", TimestampType(), True) \
            .add("return", TimestampType(), True) \
            .add("departure_id", StringType(), True) \
            .add("departure_name", StringType(), True) \
            .add("return_id", FloatType(), True) \
            .add("return_name", StringType(), True) \
            .add("distance (m)", FloatType(), True) \
            .add("duration (sec.)", FloatType(), True) \
            .add("avg_speed (km/h)", FloatType(), True) \
            .add("departure_latitude", FloatType(), True) \
            .add("departure_longitude", FloatType(), True) \
            .add("return_latitude", FloatType(), True) \
            .add("return_longitude", FloatType(), True) \
            .add("Air temperature (degC)", FloatType(), True)
        new_df = spark.read.format("csv") \
            .option("header", True) \
            .schema(my_schema) \
            .load(file_name)
        table1 = new_df.groupBy("departure_name").count().withColumnRenamed("count", "count_taken")
        table2 = new_df.groupBy("return_name").count().withColumnRenamed("count", "count_returned")
        result_table = table1.join(table2, table1["departure_name"] == table2["return_name"]).select('departure_name','count_taken','count_returned')
        result_table.toPandas().to_csv(f'{file_name}_transformed')
        new_file = os.path.join(path, f'{file_name}_transformed')
        object_name = f'{file_name}_transformed'
        bucket = 'my-bucket'
        upload_file(new_file, bucket, object_name)


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


with DAG(
    'localstack_dag',
    default_args=args,
    schedule_interval='@once',
    catchup=False
) as dag:
    task1 = PythonOperator(task_id = 'open_files_and_load',
                           python_callable = open_file,
                           provide_context = True)
    task2 = PythonOperator(task_id='transform_files_and_load',
                            python_callable=open_transform_file,
                            provide_context=True)
    task1>>task2