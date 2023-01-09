import json
import urllib.parse
import boto3
import pandas as pd
from decimal import Decimal


s3 = boto3.client('s3')
dynamo = boto3.client('dynamodb')


def lambda_handler(event, context):
    bucket = 'my-bucket'
    keys = ['2016-05', '2016-10']
    for key in keys:
        if len(key) <= 7:
            df = pd.read_csv(s3.get_object(Bucket=bucket, Key=key))
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
                                'avg_duration_by_day','avg_speed_by_month','avg_speed_by_day',
                                'avg_temperature_by_month','avg_temperature_by_day'], axis=1)
            data = json.dumps(json.loads(df.to_json(orient="records"), parse_float=Decimal))
            my_dict = {'item': data, 'table': key}
            dynamotable = dynamo.Table(key)
            for record in my_dict['item']:
                dynamotable.put_item(Item=record)
        else:
            pass

