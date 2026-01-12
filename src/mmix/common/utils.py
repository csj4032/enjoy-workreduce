import json
import logging
import random
import uuid
from datetime import datetime, timedelta
from typing import Optional

import boto3
from faker import Faker
from mysql.connector import MySQLConnection


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


def get_database_connection(secrets_: dict) -> MySQLConnection:
    return MySQLConnection(
        host=secrets_["host"],
        port=secrets_["port"],
        user=secrets_["user"],
        password=secrets_["password"],
        database=secrets_["database"]
    )


def data_quality_logs(dataframe, secrets: dict, environment: str) -> None:
    logging.info(f"Data Quality Logs: {len(dataframe)} environment: {environment}")
    connection = get_database_connection(secrets)
    columns_ = dataframe.columns
    insert_query = f"INSERT INTO data_quality_logs ({', '.join(columns_)}) VALUES ({', '.join([f'%({col_})s' for col_ in columns_])}) ON DUPLICATE KEY UPDATE value = VALUES(value)"
    data = [row.asDict() for row in dataframe.collect()]
    with connection.cursor() as cursor:
        cursor.executemany(insert_query, data)
    connection.commit()


fake = Faker("ko_KR")

countries = ["KR", "US", "JP", "SG", "DE"]
languages = {"KR": "ko", "US": "en", "JP": "ja", "SG": "en", "DE": "de"}
app_versions = ["1.0.0", "1.1.0", "1.2.0", "2.0.0"]
os_versions = {"android": ["11", "12", "13", "14"], "ios": ["15.0", "16.0", "17.0"]}
device_models = {"android": ["Galaxy S23", "Galaxy Z Fold5", "Pixel 8"], "ios": ["iPhone 13", "iPhone 14 Pro", "iPhone 15"]}
carriers = ["SKT", "KT", "LGU+", "Verizon", "AT&T"]
menus = {"home": ["banner_click", "recommend_click"], "search": ["search_submit", "filter_apply"], "book": ["detail_view", "bookmark", "purchase_click"], "mypage": ["profile_view", "settings_click"]}
contents = [{"id": "book_001", "title": "스프링 부트 핵심 가이드"}, {"id": "book_002", "title": "데이터 엔지니어링 실전"}, {"id": "book_003", "title": "Kafka 완벽 가이드"}]
referrers = ["push", "deeplink", "external_ad", "organic", None]


def generate_install():
    country = random.choice(countries)
    return {
        "id": str(uuid.uuid4()),
        "country": country,
        "language": languages[country],
        "installed_at": fake.date_between(start_date="-180d", end_date="-1d").isoformat(),
    }


def generate_device():
    os = random.choice(["android", "ios"])
    return {
        "os": os,
        "os_version": random.choice(os_versions[os]),
        "type": "mobile",
        "model": random.choice(device_models[os]),
    }


def generate_event(timestamp):
    menu = random.choice(list(menus.keys()))
    action = random.choice(menus[menu])
    content = random.choice(contents) if "view" in action else None
    return {
        "timestamp": timestamp.isoformat(),
        "type": "click" if "click" in action else "view",
        "menu": menu,
        "action": action,
        "content": {
            "id": content["id"],
            "title": content["title"],
        } if content else None,
        "referrer": random.choice(referrers),
    }


def generate_user_session_logs(log_count=30):
    user_id = f"user_{fake.random_int(1000, 9999)}"
    session_id = str(uuid.uuid4())
    session_start = datetime.now() - timedelta(minutes=random.randint(1, 120))
    install = generate_install()
    device = generate_device()
    logs = []
    current_time = session_start
    for _ in range(log_count):
        logs.append({
            "user_id": user_id,
            "session_id": session_id,
            "install": install,
            "device": device,
            "app": {"version": random.choice(app_versions)},
            "network": {
                "ip": fake.ipv4_public(),
                "carrier": random.choice(carriers)
            },
            "event": generate_event(current_time),
        })
        current_time += timedelta(seconds=random.randint(2, 15))
    return logs
