import argparse
import base64 as b64
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Size, Uniqueness
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp

from mmix.common.utils import data_quality_logs


def get_args(parser):
    parser.add_argument('--dag_id', type=str, default="dataplatform_dashboard_international_risk_daily", help="Dag ID for tracking")
    parser.add_argument('--run_id', type=str, default="", help="Run ID for tracking")
    parser.add_argument("--mysql_primary_secret", type=str, required=False, help="base64 encoded json string")
    parser.add_argument("--mysql_observability_secret", type=str, required=False, help="base64 encoded json string")
    parser.add_argument('--environment', type=str, default="prod", help="Environment for the job (e.g., dev, stg, prod)")
    parser.add_argument('--logical_datetime', type=str, default=datetime.now(tz=ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S"))
    logging.info(f"Arguments: {parser.parse_args()}")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logging.info("Example Deequ User App Log Data Quality")
    _args = get_args(argparse.ArgumentParser())
    _dag_id = _args.dag_id
    _run_id = _args.run_id
    _mysql_primary_secret = json.loads(b64.b64decode(_args.mysql_primary_secret).decode("utf-8"))
    _mysql_observability_secret = json.loads(b64.b64decode(_args.mysql_observability_secret).decode("utf-8"))
    _environment = _args.environment
    _logical_datetime = _args.logical_datetime

    with (SparkSession.builder.appName("dataplatform_dashboard_international_risk_daily").getOrCreate() as _spark):
        logger.info(f"Spark session started.")
        logger.info(f"Dag ID: {_dag_id}, Run ID: {_run_id}, Logical Datatime: {_logical_datetime}, Environment: {_environment}")
        jdbc_url = f"jdbc:mysql://{_mysql_primary_secret['host']}:{_mysql_primary_secret['port']}/{_mysql_primary_secret['database']}?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8"
        jdbc_props = {"user": _mysql_primary_secret["user"], "password": _mysql_primary_secret["password"], "driver": _mysql_primary_secret.get("driver", "com.mysql.cj.jdbc.Driver"), "fetchsize": "1000"}
        bounds_dataframe = _spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .options(**jdbc_props) \
            .option("dbtable", "(SELECT MIN(id) AS lo, MAX(id) AS hi FROM news_articles) t") \
            .load()
        row = bounds_dataframe.first()
        lower_bound = int(row["lo"] or 0)
        upper_bound = int(row["hi"] or 0)
        dataframe = _spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .options(**jdbc_props) \
            .option("dbtable", "news_articles") \
            .option("partitionColumn", "id") \
            .option("lowerBound", lower_bound) \
            .option("upperBound", upper_bound) \
            .option("numPartitions", 4) \
            .load()
        dataframe.printSchema()
        analysis_result = AnalysisRunner(_spark) \
            .onData(dataframe) \
            .addAnalyzer(Size()) \
            .addAnalyzer(Uniqueness(["id"])) \
            .addAnalyzer(Completeness("id")) \
            .addAnalyzer(Completeness(["published"])) \
            .addAnalyzer(Completeness(["title"])) \
            .run()

        analyzer_context = AnalyzerContext \
            .successMetricsAsDataFrame(_spark, analysis_result) \
            .withColumn("run_name", lit(_args.dag_id)) \
            .withColumn("run_id", lit(_args.run_id)) \
            .withColumn("logical_datetime", to_timestamp(lit(_logical_datetime), "yyyy-MM-dd HH:mm:ss"))
        analyzer_context.show()
        data_quality_logs(analyzer_context, _mysql_observability_secret, _environment)
