import argparse
import json
import logging
import random
from datetime import datetime
from zoneinfo import ZoneInfo

from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Size, Uniqueness
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, to_date, to_timestamp, base64
from pyspark.sql.types import (
    DateType
)
from pyspark.sql.types import StructType, StructField, StringType

from mmix.common.utils import data_quality_logs, generate_user_session_logs


def get_args(parser):
    parser.add_argument('--dag_id', type=str, default="dataplatform_dashboard_international_risk_daily", help="Dag ID for tracking")
    parser.add_argument('--run_id', type=str, default="", help="Run ID for tracking")
    parser.add_argument("--secret", type=str, required=False, help="base64 encoded json string")
    parser.add_argument('--environment', type=str, default="prod", help="Environment for the job (e.g., dev, stg, prod)")
    parser.add_argument('--logical_datatime', type=str, default=datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S%z"))
    logging.info(f"Arguments: {parser.parse_args()}")
    return parser.parse_args()


def get_schema():
    return StructType([
        StructField("user_id", StringType(), nullable=False),
        StructField("session_id", StringType(), nullable=False),
        StructField("install", StructType([
            StructField("id", StringType(), nullable=False),
            StructField("country", StringType(), nullable=False),
            StructField("language", StringType(), nullable=False),
            StructField("installed_at", DateType(), nullable=False), ]), nullable=False),
        StructField("device", StructType([
            StructField("os", StringType(), nullable=False),
            StructField("os_version", StringType(), nullable=False),
            StructField("type", StringType(), nullable=False),
            StructField("model", StringType(), nullable=False), ]), nullable=False),
        StructField("app", StructType([
            StructField("version", StringType(), nullable=False), ]), nullable=False),
        StructField("network", StructType([
            StructField("ip", StringType(), nullable=False), StructField("carrier", StringType(), nullable=True)
        ]), nullable=False),
        StructField("event", StructType([
            StructField("timestamp", StringType(), nullable=False),
            StructField("type", StringType(), nullable=False),
            StructField("menu", StringType(), nullable=False),
            StructField("action", StringType(), nullable=False),
            StructField("content", StructType([
                StructField("id", StringType(), nullable=True),
                StructField("title", StringType(), nullable=True), ]), nullable=True),
            StructField("referrer", StringType(), nullable=True), ]), nullable=False),
    ])


def flatten_app_log(df):
    return (
        df.select(
            "user_id",
            "session_id",
            col("install.id").alias("install_id"),
            col("install.country").alias("install_country"),
            col("install.language").alias("install_language"),
            to_date("install.installed_at").alias("install_installed_at"),
            col("device.os").alias("device_os"),
            col("device.os_version").alias("device_os_version"),
            col("device.type").alias("device_type"),
            col("device.model").alias("device_model"),
            col("app.version").alias("app_version"),
            col("network.ip").alias("network_ip"),
            col("network.carrier").alias("network_carrier"),
            to_timestamp("event.timestamp").alias("event_ts"),
            col("event.type").alias("event_type"),
            col("event.menu").alias("event_menu"),
            col("event.action").alias("event_action"),
            col("event.content.id").alias("content_id"),
            col("event.content.title").alias("content_title"),
            col("event.referrer").alias("event_referrer"),
        )
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logging.info("Example Deequ User App Log Data Quality")
    _args = get_args(argparse.ArgumentParser())
    _dag_id = _args.dag_id
    _run_id = _args.run_id
    _secret = json.loads(base64.b64decode(_args.secret).decode("utf-8"))
    _environment = _args.environment
    _logical_datatime = _args.logical_datatime

    with (SparkSession.builder.appName("dataplatform_dashboard_international_risk_daily").getOrCreate() as _spark):
        logger.info(f"Spark session started.")
        logger.info(f"Dag ID: {_dag_id}, Run ID: {_run_id}, Logical Datatime: {_logical_datatime}, Environment: {_environment}, Secret: {_secret}")
        raw_dataframe = _spark.createDataFrame(generate_user_session_logs(random.randint(100, 10000)), schema=get_schema())
        dataframe = raw_dataframe.transform(flatten_app_log)
        analysis_result = AnalysisRunner(_spark) \
            .onData(dataframe) \
            .addAnalyzer(Size()) \
            .addAnalyzer(Completeness("user_id")) \
            .addAnalyzer(Uniqueness(["session_id"])) \
            .run()

        analyzer_context = AnalyzerContext \
            .successMetricsAsDataFrame(_spark, analysis_result) \
            .withColumn("run_name", lit(_args.dag_id)) \
            .withColumn("run_id", lit(_args.run_id)) \
            .withColumn("logical_datetime", lit(_logical_datatime))
        analyzer_context.show()
        data_quality_logs(analyzer_context, _secret, _environment)
