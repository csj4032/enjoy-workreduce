import argparse
import base64 as b64
import json
import logging
from datetime import datetime
from typing import Dict, Tuple, Any
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F

from pydeequ.analyzers import AnalyzerContext, AnalysisRunner, Size, Uniqueness, Completeness, ApproxCountDistinct, Mean, Minimum, Maximum, Distinctness, Entropy
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit, to_timestamp

from mmix.common.utils import validation_results_store

_default_mysql_driver = "com.mysql.jdbc.Driver"


def get_args(parser):
    parser.add_argument('--dag_id', type=str, default="dataplatform_dashboard_international_risk_daily", help="Dag ID for tracking")
    parser.add_argument('--run_id', type=str, default="", help="Run ID for tracking")
    parser.add_argument("--mysql_mmix_secret", type=str, required=False, help="base64 encoded json string")
    parser.add_argument("--postgresql_observability_secret", type=str, required=False, help="base64 encoded json string")
    parser.add_argument('--environment', type=str, default="prod", help="Environment for the job (e.g., dev, stg, prod)")
    parser.add_argument('--logical_datetime', type=str, default=datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S"))
    logging.info(f"Arguments: {parser.parse_args()}")
    return parser.parse_args()


def decode_secret(b64_json: str) -> Dict:
    return json.loads(b64.b64decode(b64_json).decode("utf-8"))


def build_mysql_jdbc(secret: Dict) -> Tuple[str, Dict]:
    jdbc_url_ = f"jdbc:mysql://{secret['host']}:{secret['port']}/{secret['database']}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true"
    jdbc_props_ = {"user": secret["user"], "password": secret["password"], "driver": secret.get("driver", _default_mysql_driver), "fetchsize": "1000"}
    return jdbc_url_, jdbc_props_


def fetch_id_bounds(spark: SparkSession, jdbc_url_: str, jdbc_props_: Dict, table: str, id_col: str = "id") -> Tuple[int, int]:
    bounds_df = spark.read.format("jdbc") \
        .option("url", jdbc_url_) \
        .options(**jdbc_props_) \
        .option("dbtable", f"(SELECT MIN({id_col}) AS lo, MAX({id_col}) AS hi FROM {table}) t") \
        .load()
    row = bounds_df.first()
    lo = int(row["lo"] or 0)
    hi = int(row["hi"] or 0)
    return lo, hi


def read_jdbc_partitioned(spark: SparkSession, jdbc_url_: str, jdbc_props_: Dict, table: str, partition_col: str, lower_bound_: int, upper_bound_: int, num_partitions: int = 4) -> DataFrame:
    return spark.read.format("jdbc") \
        .option("url", jdbc_url_) \
        .options(**jdbc_props_) \
        .option("dbtable", table) \
        .option("partitionColumn", partition_col) \
        .option("lowerBound", lower_bound_) \
        .option("upperBound", upper_bound_) \
        .option("numPartitions", num_partitions) \
        .load()


def run_deequ_analysis(spark: SparkSession, dataframe_: DataFrame):
    return AnalysisRunner(spark) \
        .onData(dataframe_) \
        .addAnalyzer(Size()) \
        .addAnalyzer(Uniqueness(["id"])) \
        .addAnalyzer(Completeness("id")) \
        .addAnalyzer(ApproxCountDistinct("link")) \
        .addAnalyzer(ApproxCountDistinct("title")) \
        .addAnalyzer(Completeness("published")) \
        .addAnalyzer(Completeness("title")) \
        .addAnalyzer(Completeness("summary")) \
        .addAnalyzer(Completeness("description")) \
        .addAnalyzer(Distinctness("subject")) \
        .addAnalyzer(Entropy("subject")) \
        .addAnalyzer(Distinctness("keyword")) \
        .addAnalyzer(Entropy("keyword")) \
        .addAnalyzer(Mean("title_len")) \
        .addAnalyzer(Minimum("title_len")) \
        .addAnalyzer(Maximum("title_len")) \
        .addAnalyzer(Mean("summary_len")) \
        .addAnalyzer(Minimum("summary_len")) \
        .addAnalyzer(Maximum("summary_len")) \
        .addAnalyzer(Mean("description_len")) \
        .addAnalyzer(Minimum("description_len")) \
        .addAnalyzer(Maximum("description_len")) \
        .run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.info("Example Deequ User App Log Data Quality")
    _args = get_args(argparse.ArgumentParser())
    _dag_id = _args.dag_id
    _run_id = _args.run_id
    _environment = _args.environment
    _logical_datetime = _args.logical_datetime
    _mysql_mmix_secret = decode_secret(_args.mysql_mmix_secret)
    _postgresql_observability_secret = decode_secret(_args.postgresql_observability_secret)

    with SparkSession.builder.appName("dataplatform_dashboard_international_risk_daily").getOrCreate() as _spark:
        logger.info("Spark session started.")
        logger.info("Dag ID: %s, Run ID: %s, Logical Datetime: %s, Environment: %s", _dag_id, _run_id, _logical_datetime, _environment)
        logger.info("Postgresql Observability Secret: %s", _postgresql_observability_secret)
        jdbc_url, jdbc_props = build_mysql_jdbc(_mysql_mmix_secret)
        lower_bound, upper_bound = fetch_id_bounds(_spark, jdbc_url, jdbc_props, table="news_articles", id_col="id")

        if lower_bound == 0 and upper_bound == 0:
            logger.warning("news_articles is empty. Skip analyzers.")
        else:
            dataframe = read_jdbc_partitioned(_spark, jdbc_url, jdbc_props, table="news_articles", partition_col="id", lower_bound_=lower_bound, upper_bound_=upper_bound, num_partitions=4) \
                .withColumn("title_len", F.length(F.col("title"))) \
                .withColumn("summary_len", F.length(F.col("summary"))) \
                .withColumn("description_len", F.length(F.col("description")))
            dataframe.printSchema()
            analysis_result = run_deequ_analysis(_spark, dataframe)
            logger.info(f"Analysis result: {analysis_result}")
            metrics_dataframe = AnalyzerContext.successMetricsAsDataFrame(_spark, analysis_result) \
                .withColumn("run_name", lit(_dag_id)) \
                .withColumn("run_id", lit(_run_id)) \
                .withColumn("logical_datetime", to_timestamp(lit(_logical_datetime), "yyyy-MM-dd HH:mm:ss"))
            metrics_dataframe.show(truncate=False)
            validation_results_store(metrics_dataframe, _postgresql_observability_secret, _environment)
