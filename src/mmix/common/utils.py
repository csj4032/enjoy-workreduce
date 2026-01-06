import json
import logging
from datetime import datetime, timedelta
from typing import Optional

import boto3
from mysql.connector import MySQLConnection


def get_willog():
    print("This is a placeholder function for Willog. Implement your logic here.")


def get_secret_value(secret_name: str, region_name: str = "ap-northeast-2") -> Optional[list]:
    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    try:
        return json.loads(response['SecretString'])
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON for secret: {secret_name}")
        return None


def get_s3_paths(prefix: str, s3_window_hours: int, to_datatime: datetime) -> list:
    return [f"{prefix}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/hour={dt.hour:02d}" for i in range(s3_window_hours) for dt in [to_datatime - timedelta(hours=i)]]


def s3_exists(s3_client_, bucket: str, prefix: str) -> bool:
    logging.info(f"Checking S3 bucket: {bucket}, prefix: {prefix}")
    paginator = s3_client_.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                return True
    return False


def filter_valid_s3_paths(s3_client_, bucket: str, prefixes: list[str]) -> list[str]:
    return [f"s3a://{bucket}/{prefix}/*.json" for prefix in prefixes if s3_exists(s3_client_, bucket, prefix)]


def get_database_connection(secrets_: dict, environment_: str, service: str) -> MySQLConnection:
    return MySQLConnection(
        host=secrets_[f'{environment_}-{service}-host'],
        port=secrets_[f'{environment_}-{service}-port'],
        user=secrets_[f'{environment_}-{service}-user'],
        password=secrets_[f'{environment_}-{service}-password'],
        database=secrets_[f'{environment_}-{service}-database']
    )


def etl_analysis_logs(dataframe, secrets, environment="prod", service="data-aurora") -> None:
    connection = get_database_connection(secrets, environment, service)
    columns_ = dataframe.columns
    insert_query = f"INSERT INTO etl_analysis_logs ({', '.join(columns_)}) VALUES ({', '.join([f'%({col_})s' for col_ in columns_])}) ON DUPLICATE KEY UPDATE value = VALUES(value)"
    data = [row.asDict() for row in dataframe.collect()]
    with connection.cursor() as cursor:
        cursor.executemany(insert_query, data)
    connection.commit()
