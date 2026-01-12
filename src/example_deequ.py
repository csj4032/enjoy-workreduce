import argparse
import logging
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from faker import Faker
from pyspark.sql import SparkSession


def get_args(parser):
    parser.add_argument('--dag_id', type=str, default="dataplatform_dashboard_international_risk_daily", help="Dag ID for tracking")
    parser.add_argument('--run_id', type=str, default="", help="Run ID for tracking")
    parser.add_argument('--logical_datatime', type=str, default=datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S%z"))
    parser.add_argument('--environment', type=str, default="prod", help="Environment for the job (e.g., dev, stg, prod)")
    logging.info(f"Arguments: {parser.parse_args()}")
    return parser.parse_args()


def data_generation(fake=Faker("ko_KR"), n: int = 1000) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "id": i + 1,
            "name": fake.name(),
            "age": fake.random_int(min=20, max=65),
            "weight": fake.random_int(min=10, max=250),
            "height": fake.random_int(min=10, max=250),
            "gender": random.choice(['남성', '여성']),
            "address": fake.address(),
            "job": fake.job(),
            "email": fake.email(),
            "signup": (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
        })
    return pd.DataFrame(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logging.info("dataplatform_dashboard_international_risk_hourly")
    _args = get_args(argparse.ArgumentParser())
    _dag_id = _args.dag_id
    _run_id = _args.run_id
    _logical_datatime = _args.logical_datatime
    _environment = _args.environment

    with (SparkSession.builder.appName("dataplatform_dashboard_international_risk_daily").getOrCreate() as _spark):
        logger.info(f"Spark session started.")
        logger.info(f"Dag ID: {_dag_id}, Run ID: {_run_id}, Logical Datatime: {_logical_datatime}, Environment: {_environment}")
