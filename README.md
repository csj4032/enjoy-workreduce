# enjoy-workreduce

MMIX Data Engineering을 위한 AWS EMR Serverless Python 패키지입니다. PySpark 기반의 데이터 처리와 PyDeequ를 활용한 데이터 품질 검증 기능을 제공합니다.

## 최근 변경사항 (2026-01-17)

### 신규 기능

1. **Apache Flink 지원 추가**
   - PyFlink 1.20.3 라이브러리 추가
   - `example_flink.ipynb`: Kafka CDC 기반 실시간 스트리밍 처리 예제
   - Flink JobManager와의 연동 및 체크포인팅 설정 포함

2. **Apache Iceberg 지원 추가**
   - `example_iceberg.ipynb`: Apache Iceberg 테이블 포맷 처리 예제
   - 데이터 레이크하우스 아키텍처 지원

3. **데이터 품질 분석 강화**
   - `example_spark_mysql.py`에 추가 PyDeequ Analyzer 구현:
     - `Completeness`: 필수 컬럼 완전성 검사 (id, published, title, summary, description)
     - `Distinctness`: 고유값 비율 측정 (subject, keyword)
     - `Entropy`: 데이터 분포 엔트로피 분석 (subject, keyword)
   - 텍스트 길이 통계 분석 (Mean, Min, Max)

### 개선사항

1. **Notebook 코드 정리**
   - `example_elasticsearch.ipynb`: 불필요한 주석 제거 및 실행 타임스탬프 업데이트
   - `example_s3.ipynb`: Spark 세션 초기화 로직 리팩토링 및 가독성 개선
   - `example_kafka.ipynb`: 코드 구조 개선 및 주석 업데이트

2. **코드 리팩토링**
   - `example_spark_mysql.py`: 함수 시그니처 개선 및 로깅 강화
   - `example_spark_deequ.py`: 중복 analyzer 제거 (session_id uniqueness)

## 프로젝트 개요

이 프로젝트는 다음과 같은 기능을 제공합니다:

- **데이터 품질 검증**: PyDeequ를 사용한 데이터 완전성, 고유성, 규칙 준수 검사
- **데이터 품질 검증 (GX)**: Great Expectations를 활용한 Spark DataFrame 검증 및 Data Docs 생성
- **AWS 통합**: S3, Secrets Manager, MySQL, OpenSearch와의 연동
- **테스트 데이터 생성**: Faker를 활용한 한국어 로케일 기반 합성 데이터 생성
- **대용량 처리**: PySpark 기반 분산 데이터 처리 및 파티셔닝
- **스트리밍 처리**: Kafka 연동 및 Avro 스키마 지원
- **실시간 스트리밍**: Apache Flink를 활용한 실시간 데이터 스트리밍 처리
- **데이터 레이크하우스**: Apache Iceberg 테이블 포맷 지원

## 환경 설정

### 1. Conda 환경 생성

Python 3.10.12 기반의 conda 환경을 생성하고 활성화합니다.

```bash
conda create -n enjoy-workreduce python==3.10.12
conda activate enjoy-workreduce
```

### 2. 의존성 설치

필요한 패키지들을 설치합니다.

```bash
pip install -r requirements.txt
```

### 3. 추가 패키지 설치 (선택사항)

Jupyter 환경에서 작업하거나 EMR Serverless 인증이 필요한 경우:

```bash
pip install sparkmagic emr-serverless-customauth
```

### 4. PySpark JAR 파일 경로

사용자 정의 JAR 파일이 필요한 경우, 다음 경로에 추가합니다:

```bash
# Anaconda 사용 시
cd /opt/anaconda3/envs/enjoy-workreduce/lib/python3.10/site-packages/pyspark/jars/

# Miniconda 사용 시
cd /opt/miniconda3/envs/enjoy-workreduce/lib/python3.10/site-packages/pyspark/jars/
```

## 패키지 빌드

### 초기 설정

빌드 도구를 설치합니다.

```bash
pip install build
pip install --upgrade --force-reinstall setuptools
```

### 패키지 빌드

프로젝트를 빌드하면 `dist/` 디렉토리에 배포 가능한 wheel 파일이 생성됩니다.

```bash
python -m build
```

빌드 결과:
- `dist/enjoy_workreduce-0.0.1-py3-none-any.whl`
- `dist/enjoy-workreduce-0.0.1.tar.gz`

## 예제 스크립트

### example_spark_deequ.py

**목적**: Faker로 생성한 합성 사용자 앱 로그 데이터에 대해 PyDeequ 데이터 품질 분석을 수행합니다.

#### 주요 기능

1. **합성 데이터 생성**
   - `generate_user_session_logs()` 함수로 사용자 세션 로그 생성
   - 한국어 로케일 기반의 현실적인 테스트 데이터
   - 중첩된 JSON 스키마 구조:
     - User: user_id
     - Session: session_id
     - Install: id, country, language, installed_at
     - Device: os, os_version, type, model
     - App: version
     - Network: ip, carrier
     - Event: timestamp, type, menu, action, content, referrer

2. **데이터 변환**
   - `flatten_app_log()`: 중첩 구조를 평탄화하여 분석 가능한 테이블 형태로 변환
   - Timestamp 및 Date 타입 변환

3. **PyDeequ 분석기 적용**
   - **Size**: 전체 레코드 수
   - **Completeness**: 필수 컬럼의 완전성 (user_id, session_id, install_id, device_os 등)
   - **Uniqueness**: 고유성 검사 (session_id 단독, session_id + event_ts 조합)
   - **Distinctness**: session_id의 고유값 비율
   - **ApproxCountDistinct**: user_id의 근사 고유값 개수
   - **Histogram**: 분포 분석 (install_country, event_type, event_menu)
   - **Entropy**: event_type의 엔트로피
   - **Compliance**: 규칙 준수 검사
     - IPv4 주소 형식 유효성 검증
     - SemVer 버전 형식 유효성 검증 (app_version)
     - 이벤트 시간이 설치 시간 이후인지 검증

4. **결과 저장**
   - 분석 메트릭을 MySQL 데이터베이스에 저장
   - `run_name`, `run_id`, `logical_datetime`으로 실행 이력 추적

#### 실행 방법

```bash
spark-submit \
  --py-files dist/enjoy_workreduce-0.0.1-py3-none-any.whl \
  src/example_spark_deequ.py \
  --dag_id "dataplatform_dashboard_international_risk_daily" \
  --run_id "manual_run_001" \
  --secret "$(echo '{"host":"mysql-host.com","port":3306,"user":"admin","password":"pass","database":"observability"}' | base64)" \
  --environment "prod" \
  --logical_datetime "2026-01-17 12:00:00"
```

#### 파라미터 설명

| 파라미터 | 필수 여부 | 기본값 | 설명 |
|---------|----------|--------|------|
| `--dag_id` | 선택 | `dataplatform_dashboard_international_risk_daily` | Airflow DAG ID (추적용) |
| `--run_id` | 선택 | `""` | 실행 ID (추적용) |
| `--secret` | 선택 | - | Base64 인코딩된 MySQL 접속 정보 (JSON) |
| `--environment` | 선택 | `prod` | 환경 구분 (dev/stg/prod) |
| `--logical_datetime` | 선택 | 현재 UTC 시간 | 논리적 실행 시간 |

#### Secret JSON 형식

```json
{
  "host": "mysql-host.example.com",
  "port": 3306,
  "user": "username",
  "password": "password",
  "database": "observability"
}
```

Base64 인코딩 방법:
```bash
echo '{"host":"mysql-host.com","port":3306,"user":"admin","password":"pass","database":"observability"}' | base64
```

---

### example_spark_mysql.py

**목적**: MySQL 데이터베이스의 `news_articles` 테이블에서 데이터를 읽어와 PyDeequ 데이터 품질 검증을 수행합니다.

#### 주요 기능

1. **JDBC 연결 설정**
   - `build_mysql_jdbc()`: MySQL JDBC URL 및 연결 속성 생성
   - 설정 포함: SSL 비활성화, UTC 타임존, UTF-8 인코딩
   - 커스텀 JDBC 드라이버 지원

2. **동적 파티셔닝 전략**
   - `fetch_id_bounds()`: 테이블의 MIN/MAX ID를 조회하여 범위 확인
   - `read_jdbc_partitioned()`: ID 범위 기반으로 데이터를 파티션으로 나누어 병렬 읽기
   - `numPartitions` 파라미터로 병렬 처리 수준 조정 (기본값: 4)
   - 대용량 테이블 처리 최적화

3. **데이터 전처리**
   - 문자열 길이 컬럼 생성: `title_len`, `summary_len`, `description_len`
   - PySpark의 `F.length()` 함수로 각 텍스트 필드의 길이 계산

4. **PyDeequ 분석기 적용**
   - **Size**: 전체 레코드 수
   - **Uniqueness**: id 컬럼의 고유성 검사
   - **Completeness**: 필수 컬럼 완전성 검사 (id, published, title, summary, description)
   - **ApproxCountDistinct**: 고유값 근사 개수 (link, title)
   - **Distinctness**: 고유값 비율 (subject, keyword)
   - **Entropy**: 엔트로피 분석 (subject, keyword)
   - **Mean, Minimum, Maximum**: 텍스트 길이 통계 (title_len, summary_len, description_len)

5. **결과 저장**
   - 분석 메트릭을 별도의 관찰성(observability) MySQL 데이터베이스에 저장
   - 실행 이력 추적 (run_name, run_id, logical_datetime)
   - 소스 DB와 메트릭 저장 DB 분리

#### 실행 방법

```bash
spark-submit \
  --jars /path/to/mysql-connector-java.jar \
  --py-files dist/enjoy_workreduce-0.0.1-py3-none-any.whl \
  src/example_spark_mysql.py \
  --dag_id "dataplatform_dashboard_international_risk_daily" \
  --run_id "manual_run_001" \
  --mysql_mmix_secret "$(echo '{"host":"source-db.com","port":3306,"user":"admin","password":"pass","database":"mmix"}' | base64)" \
  --mysql_observability_secret "$(echo '{"host":"metrics-db.com","port":3306,"user":"admin","password":"pass","database":"observability"}' | base64)" \
  --environment "prod" \
  --logical_datetime "2026-01-17 12:00:00"
```

#### 파라미터 설명

| 파라미터 | 필수 여부 | 기본값 | 설명 |
|---------|----------|--------|------|
| `--dag_id` | 선택 | `dataplatform_dashboard_international_risk_daily` | Airflow DAG ID (추적용) |
| `--run_id` | 선택 | `""` | 실행 ID (추적용) |
| `--mysql_mmix_secret` | 선택 | - | Base64 인코딩된 소스 MySQL 접속 정보 |
| `--mysql_observability_secret` | 선택 | - | Base64 인코딩된 메트릭 저장 MySQL 접속 정보 |
| `--environment` | 선택 | `prod` | 환경 구분 (dev/stg/prod) |
| `--logical_datetime` | 선택 | 현재 UTC 시간 | 논리적 실행 시간 |

#### Secret JSON 형식

```json
{
  "host": "mysql-host.example.com",
  "port": 3306,
  "user": "username",
  "password": "password",
  "database": "database_name",
  "driver": "com.mysql.jdbc.Driver"
}
```

#### 분석 대상 테이블 구조

`news_articles` 테이블:
- **id**: 기본 키
- **link**: 뉴스 기사 링크
- **title**: 제목
- **published**: 발행일
- **summary**: 요약
- **description**: 본문
- **subject**: 주제 분류
- **keyword**: 키워드

#### 특징 및 주의사항

- **빈 테이블 처리**: 레코드가 없을 경우 분석을 건너뛰고 경고 로그 출력
- **파티션 조정**: `numPartitions` 값을 조정하여 병렬 처리 수준 제어 가능
- **두 DB 분리**: 소스 데이터베이스와 메트릭 저장 데이터베이스를 분리하여 운영
- **JDBC JAR 필요**: MySQL JDBC 드라이버 JAR 파일을 `--jars` 옵션으로 지정 필요
- **텍스트 길이 분석**: title, summary, description의 길이를 계산하여 통계 분석 수행
- **엔트로피 분석**: subject와 keyword의 분포 엔트로피를 계산하여 데이터 다양성 측정

---

## 공통 유틸리티 (mmix.common.utils)

`mmix.common.utils` 모듈은 자주 사용되는 유틸리티 함수들을 제공합니다.

### 1. AWS Secrets Manager 연동

AWS Secrets Manager에서 비밀 정보를 조회합니다.

```python
from mmix.common.utils import get_secret_value

# Secrets Manager에서 비밀 정보 가져오기
secrets = get_secret_value("my-secret-name", region_name="ap-northeast-2")
print(secrets)  # {'host': '...', 'port': 3306, ...}
```

### 2. S3 경로 생성 및 검증

시간 기반 S3 경로를 생성하고 실제로 존재하는 경로만 필터링합니다.

```python
import boto3
from datetime import datetime
from mmix.common.utils import get_s3_paths, filter_valid_s3_paths

# 최근 24시간의 S3 경로 생성 (시간별 파티션)
prefix = "s3://my-bucket/logs"
paths = get_s3_paths(prefix, s3_window_hours=24, to_datatime=datetime.now())
# 결과: ['s3://my-bucket/logs/year=2026/month=01/day=17/hour=12', ...]

# 실제로 데이터가 있는 경로만 필터링
s3_client = boto3.client('s3')
valid_paths = filter_valid_s3_paths(s3_client, "my-bucket", paths)
# 결과: 실제 .json 파일이 존재하는 경로만 반환
```

### 3. 테스트 데이터 생성

한국어 로케일 기반의 현실적인 사용자 세션 로그 데이터를 생성합니다.

```python
from mmix.common.utils import generate_user_session_logs

# 100개의 합성 사용자 세션 로그 생성
logs = generate_user_session_logs(log_count=100)

# 각 로그는 다음 정보를 포함:
# - user_id, session_id
# - install 정보 (country, language, installed_at)
# - device 정보 (os, os_version, model)
# - app 정보 (version)
# - network 정보 (ip, carrier)
# - event 정보 (timestamp, type, menu, action, content, referrer)
```

생성되는 데이터의 특징:
- 국가: KR, US, JP, SG, DE
- OS: Android (11-14), iOS (15.0-17.0)
- 앱 버전: 1.0.0, 1.1.0, 1.2.0, 2.0.0
- 메뉴: home, search, book, mypage
- 이벤트: 클릭, 조회, 검색 등

### 4. 데이터 품질 로깅

PyDeequ 분석 결과를 MySQL 데이터베이스에 저장합니다.

```python
from pyspark.sql import SparkSession
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Size, Completeness
from mmix.common.utils import data_quality_logs

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("s3://bucket/data/")

# PyDeequ 분석 실행
analysis_result = AnalysisRunner(spark) \
    .onData(df) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("user_id")) \
    .run()

# 메트릭을 DataFrame으로 변환
metrics_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)

# MySQL에 저장 (ON DUPLICATE KEY UPDATE 사용)
mysql_secrets = {
    "host": "mysql-host.com",
    "port": 3306,
    "user": "admin",
    "password": "password",
    "database": "observability"
}
data_quality_logs(metrics_df, mysql_secrets, environment="prod")
```

### 5. MySQL 연결

MySQL 데이터베이스 연결을 생성합니다.

```python
from mmix.common.utils import get_database_connection

secrets = {
    "host": "mysql-host.com",
    "port": 3306,
    "user": "admin",
    "password": "password",
    "database": "mydb"
}

connection = get_database_connection(secrets)
cursor = connection.cursor()
cursor.execute("SELECT * FROM my_table LIMIT 10")
results = cursor.fetchall()
connection.close()
```

## Jupyter Notebooks

프로젝트에는 다양한 환경에서 실행 가능한 Jupyter 노트북 예제가 포함되어 있습니다.

### notebooks/emr/
AWS EMR Serverless 환경에서 실행 가능한 예제:
- `example_deequ.ipynb`: EMR에서 PyDeequ 실행
- `example_s3.ipynb`: EMR에서 S3 데이터 처리

### notebooks/spark/
Spark 클러스터 환경에서 실행 가능한 예제:
- `01_example_spark.ipynb`: PySpark 기본 사용법
- `02_example_connection.ipynb`: 연결 테스트
- `03_example_deequ.ipynb`: PyDeequ 데이터 품질 검증
- `03_example_gx.ipynb`: Great Expectations 데이터 품질 검증
- `04_example_mysql.ipynb`: MySQL 연동
- `05_example_s3.ipynb`: S3 데이터 읽기/쓰기
- `06_example_iceberg.ipynb`: Apache Iceberg 테이블 포맷 처리
- `07_example_deltalake.ipynb`: Delta Lake 테이블 포맷 처리
- `08_example_hudi.ipynb`: Apache Hudi 테이블 포맷 처리
- `10_example_kafka_s3_sync_read.ipynb`: Kafka + S3 동기 읽기
- `11_example_kafka_flink_s3_sync_load.ipynb`: Kafka + Flink + S3 동기 로드
- `12_example_kafka_write_stream_to_s3.ipynb`: Kafka 스트림을 S3에 쓰기
- `99_example_elasticsearch.ipynb`: Elasticsearch 연동

### notebooks/flink/
Apache Flink 스트리밍 처리 예제:
- `01_example_flink.ipynb`: Flink 기본 스트리밍 처리

## AWS 배포

### S3에 배포 (Production AWS)

소스 코드와 빌드된 패키지를 S3에 업로드합니다.

```bash
# 소스 코드 업로드
aws s3 cp ./src s3://mmix-prod-dataengineer-workreduce/src --recursive --profile mmix-genius

# 빌드된 패키지 업로드
aws s3 cp ./dist s3://mmix-prod-dataengineer-workreduce/dist --recursive --profile mmix-genius
```

### S3에 배포 (Local Minio)

로컬 Minio 환경에 배포하는 경우:

```bash
aws s3 cp ./src s3://mmix-prod-dataengineer-workreduce/src --recursive --endpoint-url http://minio:9000 --profile minio
```

## 테스트

### 기본 테스트 실행

```bash
pytest
```

### 커스텀 OpenSearch 호스트로 테스트

```bash
pytest --host=custom-opensearch-host.com --port=443
```

### 특정 테스트 파일 실행

```bash
pytest tests/test_specific.py -v
```

### 로그 레벨 조정

`pytest.ini` 파일에서 로그 레벨이 INFO로 설정되어 있습니다. 필요에 따라 DEBUG로 변경할 수 있습니다.

## 주요 의존성

| 패키지 | 버전 | 용도 |
|--------|------|------|
| pyspark | - | 분산 데이터 처리 프레임워크 |
| pydeequ | 1.5.0 | 데이터 품질 검증 (Amazon Deequ 기반) |
| boto3 | 1.37.2 | AWS SDK (S3, Secrets Manager) |
| Faker | 40.1.0 | 합성 테스트 데이터 생성 |
| confluent-kafka | 2.12.2 | Kafka 스트리밍 처리 |
| apache-flink | 1.20.3 | 실시간 스트리밍 데이터 처리 |
| apache-beam | 2.48.0 | 배치/스트리밍 통합 데이터 처리 |
| mysql-connector-python | 9.3.0 | MySQL 데이터베이스 연결 |
| pymysql | 1.1.2 | MySQL 데이터베이스 연결 (대안) |
| psycopg2-binary | 2.9.11 | PostgreSQL 데이터베이스 연결 |
| SQLAlchemy | 1.4.54 | SQL 툴킷 및 ORM |
| elasticsearch | 8.19.2 | Elasticsearch 연동 |
| duckdb | 1.3.0 | 임베디드 분석 데이터베이스 |
| pandas | 2.1.4 | 데이터 분석 및 처리 |
| numpy | 1.24.4 | 수치 계산 |
| great-expectations | 1.11.0 | 데이터 품질 검증 프레임워크 |

## PyDeequ Spark 클러스터 예제

`notebooks/spark/03_example_deequ.ipynb`은 Spark 클러스터 환경에서 PyDeequ를 활용한 데이터 품질 검증 예제입니다.

### 주요 기능

#### 1. Faker 기반 테스트 데이터 생성

한국어 로케일(`ko_KR`)을 사용하여 현실적인 사용자 데이터를 생성합니다.

```python
from faker import Faker

def generate_users(fake: Faker, count: int):
    rows = []
    for i in range(1, count + 1):
        rows.append({
            "user_id": i,
            "email": fake.unique.email(),
            "name": fake.name(),
            "age": fake.random_int(min=10, max=100),
            "gender": random.choice(["M", "F"]),
            "job": fake.job(),
            "address": fake.address(),
            "signup": (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
            "created_at": datetime.now()
        })
    return rows

users = spark.createDataFrame(data=generate_users(Faker("ko_KR"), 100), schema=user_schema)
```

#### 2. S3 (Minio) 연동 Spark 세션

로컬 Minio 환경과 연동하여 메트릭 결과를 S3에 저장합니다.

```python
spark = SparkSession.builder \
    .appName("PyDeequ Example") \
    .master("spark://spark-master.mmix.io:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.mmix.io:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "mmix") \
    .config("spark.hadoop.fs.s3a.secret.key", "mmixmmix") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "1") \
    .getOrCreate()
```

#### 3. FileSystemMetricsRepository

PyDeequ의 메트릭 결과를 S3에 JSON 형식으로 저장합니다.

```python
from pydeequ.repository import FileSystemMetricsRepository, ResultKey

repository = FileSystemMetricsRepository(
    spark,
    "s3a://mmix-prod-dataengineer-validation/deequ/sample/metrics/orders/metrics.json"
)
resultKey = ResultKey(
    spark,
    ResultKey.current_milli_time(),
    {"pipeline": "orders", "dataset": "orders", "env": "prod"}
)
```

#### 4. VerificationSuite (데이터 검증)

데이터 품질 규칙을 정의하고 검증합니다. `satisfies`를 사용하여 커스텀 SQL 조건을 검사할 수 있습니다.

```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

expected_count = 100
email_regex = r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"

check = Check(spark, CheckLevel.Error, "Basic data checks") \
    .hasSize(lambda n: n == expected_count) \
    .isComplete("user_id") \
    .isComplete("email") \
    .isComplete("name") \
    .isComplete("created_at") \
    .isUnique("user_id") \
    .isUnique("email") \
    .satisfies("gender IS NULL OR gender IN ('M','F')", "gender_in_domain_or_null", lambda ratio: ratio == 1.0) \
    .satisfies("age IS NULL OR (age >= 10 AND age <= 100)", "age_range_or_null", lambda ratio: ratio == 1.0) \
    .satisfies(f"email RLIKE '{email_regex}'", "email_format", lambda ratio: ratio >= 0.99) \
    .satisfies("signup IS NULL OR signup <= current_date()", "signup_not_in_future", lambda ratio: ratio == 1.0) \
    .satisfies("signup IS NULL OR created_at >= signup", "created_at_after_signup", lambda ratio: ratio == 1.0)

# Warning 레벨 체크 추가
warn_check = Check(spark, CheckLevel.Warning, "Users dataset warning checks") \
    .satisfies("CASE WHEN gender IS NULL THEN true ELSE true END", "noop_for_example", lambda _: True)

check_result = VerificationSuite(spark) \
    .onData(users) \
    .addCheck(check) \
    .addCheck(warn_check) \
    .useRepository(repository) \
    .saveOrAppendResult(resultKey) \
    .run()

# 검증 결과를 DataFrame으로 확인
check_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
check_df.show(truncate=False)
```

#### 5. 데이터 변환 (파생 컬럼 생성)

분석을 위해 파생 컬럼을 생성합니다.

```python
from pyspark.sql.functions import when, col, to_date, lit, datediff

# 가입일부터 생성일까지의 일수 계산
users_enriched = users.withColumn(
    "days_from_signup_to_created",
    when(col("signup").isNotNull(),
         datediff(to_date(col("created_at")), col("signup"))
    ).otherwise(lit(None))
)
```

#### 6. AnalysisRunner (데이터 분석)

다양한 분석기를 사용하여 데이터 품질 메트릭을 수집합니다.

```python
from pydeequ.analyzers import (
    AnalysisRunner, AnalyzerContext, Size, Completeness,
    ApproxCountDistinct, Distinctness, Minimum, Maximum, Mean, StandardDeviation
)

analysis_result = AnalysisRunner(spark) \
    .onData(users_enriched) \
    .addAnalyzer(Size()) \
    .addAnalyzer(Completeness("user_id")) \
    .addAnalyzer(Completeness("email")) \
    .addAnalyzer(Completeness("name")) \
    .addAnalyzer(Completeness("age")) \
    .addAnalyzer(Completeness("gender")) \
    .addAnalyzer(Completeness("job")) \
    .addAnalyzer(Completeness("address")) \
    .addAnalyzer(Completeness("signup")) \
    .addAnalyzer(Completeness("created_at")) \
    .addAnalyzer(ApproxCountDistinct("user_id")) \
    .addAnalyzer(ApproxCountDistinct("email")) \
    .addAnalyzer(Distinctness("user_id")) \
    .addAnalyzer(Distinctness("email")) \
    .addAnalyzer(Minimum("age")) \
    .addAnalyzer(Maximum("age")) \
    .addAnalyzer(Mean("age")) \
    .addAnalyzer(StandardDeviation("age")) \
    .addAnalyzer(Minimum("days_from_signup_to_created")) \
    .addAnalyzer(Maximum("days_from_signup_to_created")) \
    .addAnalyzer(Mean("days_from_signup_to_created")) \
    .useRepository(repository) \
    .saveOrAppendResult(resultKey) \
    .run()

# 메트릭을 DataFrame으로 확인
metrics_df = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_result)
metrics_df.show(truncate=False)
```

### 사용자 데이터 스키마

```python
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType, DateType, TimestampType

user_schema = StructType([
    StructField("user_id", LongType(), False),
    StructField("email", StringType(), False),
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("job", StringType(), True),
    StructField("address", StringType(), True),
    StructField("signup", DateType(), True),
    StructField("created_at", TimestampType(), True)
])
```

### 메트릭 저장 경로

| 항목 | 경로 |
|------|------|
| 메트릭 저장소 | `s3a://mmix-prod-dataengineer-validation/deequ/sample/metrics/orders/metrics.json` |

### PyDeequ 분석기 (Analyzer)

| 분석기 | 설명 |
|--------|------|
| Size | 전체 레코드 수 |
| Completeness | 컬럼의 비어있지 않은 값 비율 |
| ApproxCountDistinct | 고유값의 근사 개수 (HyperLogLog 사용) |
| Distinctness | 고유값 비율 (고유값 수 / 전체 행 수) |
| Minimum | 수치 컬럼의 최솟값 |
| Maximum | 수치 컬럼의 최댓값 |
| Mean | 수치 컬럼의 평균값 |
| StandardDeviation | 수치 컬럼의 표준편차 |
| Correlation | 두 수치 컬럼 간의 상관관계 |

### PyDeequ 검증 체크 (Check)

| 체크 | 설명 |
|------|------|
| hasSize | 레코드 수가 조건을 만족하는지 확인 |
| isComplete | 컬럼에 NULL 값이 없는지 확인 |
| isUnique | 컬럼 값이 고유한지 확인 |
| satisfies | 커스텀 SQL 조건을 만족하는지 확인 (도메인 검사, 범위 검사, 정규식 검사 등) |

## Great Expectations 설정

프로젝트는 Great Expectations 1.11.0을 활용한 데이터 품질 검증 인프라를 포함합니다.

### 디렉토리 구조

```
notebooks/spark/great_expectations/
├── great_expectations.yml           # 메인 설정 파일
├── plugins/
│   └── custom_data_docs/
│       └── styles/
│           └── data_docs_custom_styles.css
└── uncommitted/
    └── config_variables.yml         # 환경 변수 설정 (PostgreSQL 연결 정보)
```

### 주요 구성 요소

#### 1. 데이터 소스 (Spark)

```yaml
fluent_datasources:
  news_datasource:
    type: spark
    assets:
      news_datasource_asset:
        type: dataframe
        batch_definitions:
          news_datasource_batch_definition_whole_dataframe:
            partitioner:
```

런타임 Spark DataFrame을 검증 대상으로 사용합니다.

#### 2. Store 구성

검증 결과와 메타데이터를 PostgreSQL과 Minio S3에 저장합니다.

| Store | 저장소 | 테이블/경로 |
|-------|--------|------------|
| expectations_store | PostgreSQL | ge_expectations_store |
| validation_results_store | PostgreSQL | ge_validation_results_store |
| validation_results_store_minio | Minio S3 | mmix-prod-dataengineer-validation/gx/validation_results_store/ |
| checkpoint_store | PostgreSQL | ge_checkpoint_store |
| validation_definition_store | PostgreSQL | ge_validation_definition_store |
| Data Docs (minio_site) | Minio S3 | mmix-prod-dataengineer-validation/gx/data_docs_sites/ |

#### 3. 환경 변수 설정

`uncommitted/config_variables.yml`:

```yaml
validation_results_db:
  drivername: postgresql+psycopg2
  username: validations
  password: validations
  host: postgres.mmix.io
  port: 5432
  database: validations
```

### 사용 예제

`03_example_gx.ipynb` 노트북에서 뉴스 데이터 검증 워크플로우를 확인할 수 있습니다.

#### 1. 테스트 데이터 생성

```python
from faker import Faker

def generate_news_row(fake: Faker, id_: int):
    published = random_dt(14)
    return {
        "id": id_,
        "published": published,
        "subject": random.choice(["경제", "사회", "정치", "IT", "국제", "문화"]),
        "keyword": ", ".join(fake.words(nb=random.randint(2, 6)))[:200],
        "title": fake.sentence(nb_words=12)[:1000],
        "summary": fake.text(max_nb_chars=600)[:4000],
        "description": fake.text(max_nb_chars=2500),
        "original_link": fake.url()[:500],
        "link": fake.url()[:500],
        "created_at": published,
        "updated_at": published,
    }

news = spark.createDataFrame(generate_news_data(Faker("ko_KR"), 10), schema=schema)
```

#### 2. Great Expectations 컨텍스트 및 데이터소스 설정

```python
import great_expectations as gx

context = gx.get_context(
    mode="file",
    context_root_dir="/path/to/great_expectations"
)

# 데이터소스 및 자산 설정
news_datasource = context.data_sources.add_spark(name="news_datasource")
df_asset = news_datasource.add_dataframe_asset(name="news_datasource_asset")
batch_def = df_asset.add_batch_definition_whole_dataframe(name="news_datasource_batch_definition_whole_dataframe")
```

#### 3. Expectation Suite 생성

다양한 데이터 품질 규칙을 정의합니다.

```python
suite = context.suites.add(gx.ExpectationSuite(name="news_datasource_suite"))

# 필수 컬럼 존재 여부 확인
required_columns = ["id", "published", "subject", "keyword", "title", "summary",
                    "description", "original_link", "link", "created_at", "updated_at"]
for c in required_columns:
    suite.add_expectation(gx.expectations.ExpectColumnToExist(column=c))

# NULL 값 검사 (mostly 옵션으로 허용 비율 설정)
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="title", mostly=0.99))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="link", mostly=0.99))

# 고유성 검사
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

# 값 범위 검사
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="id", min_value=1))

# 허용된 값 집합 검사
allowed_subjects = ["경제", "사회", "정치", "IT", "국제", "문화"]
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="subject", value_set=allowed_subjects, mostly=0.98
))

# 문자열 길이 검사
suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToBeBetween(
    column="title", min_value=5, max_value=1000, mostly=0.99
))
suite.add_expectation(gx.expectations.ExpectColumnValueLengthsToBeBetween(
    column="summary", min_value=0, max_value=4000, mostly=0.99
))

# 정규식 패턴 검사 (URL 형식)
url_regex = r"^https?://.+"
suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(
    column="link", regex=url_regex, mostly=0.99
))

# 컬럼 간 비교 검사
suite.add_expectation(gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="updated_at", column_B="created_at", or_equal=True, mostly=0.99
))
suite.add_expectation(gx.expectations.ExpectColumnPairValuesToBeEqual(
    column_A="created_at", column_B="published", mostly=0.99
))

suite.save()
```

#### 4. Validation Definition 및 실행

```python
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="news_datasource_validation_definition",
        data=batch_def,
        suite=suite
    )
)

validation_results = validation_definition.run(
    batch_parameters={"dataframe": news},
    result_format={"result_format": "COMPLETE"}
)
```

#### 5. Checkpoint 실행 및 Data Docs 업데이트

```python
from great_expectations.checkpoint import Checkpoint

actions = [
    {"name": "update_data_docs", "type": "update_data_docs", "site_names": ["minio_site"]}
]

checkpoint = Checkpoint(
    name="news_datasource_runtime_checkpoint",
    validation_definitions=[{"name": "news_datasource_validation_definition"}],
    actions=actions
)
checkpoint = context.checkpoints.add(checkpoint)

result = checkpoint.run(batch_parameters={"dataframe": news})
```

### 지원되는 Expectations

| Expectation | 설명 | 주요 옵션 |
|-------------|------|----------|
| ExpectColumnToExist | 컬럼 존재 여부 확인 | column |
| ExpectColumnValuesToNotBeNull | NULL 값이 없는지 확인 | column, mostly |
| ExpectColumnValuesToBeUnique | 컬럼 값의 고유성 검사 | column |
| ExpectColumnValuesToBeBetween | 값이 지정 범위 내에 있는지 확인 | column, min_value, max_value |
| ExpectColumnValuesToBeInSet | 값이 허용된 집합에 포함되는지 확인 | column, value_set, mostly |
| ExpectColumnValueLengthsToBeBetween | 문자열 길이가 범위 내인지 확인 | column, min_value, max_value, mostly |
| ExpectColumnValuesToMatchRegex | 정규식 패턴과 일치하는지 확인 | column, regex, mostly |
| ExpectColumnPairValuesAToBeGreaterThanB | 컬럼 A 값이 컬럼 B보다 큰지 확인 | column_A, column_B, or_equal, mostly |
| ExpectColumnPairValuesToBeEqual | 두 컬럼 값이 같은지 확인 | column_A, column_B, mostly |

### PyDeequ vs Great Expectations

| 기능 | PyDeequ | Great Expectations |
|------|---------|-------------------|
| 설정 방식 | 프로그래매틱 | YAML/JSON 선언적 |
| 결과 저장 | FileSystemMetricsRepository (S3) | Store 내장 지원 (PostgreSQL, S3) |
| Data Docs | 없음 | HTML 보고서 자동 생성 |
| Spark 지원 | 네이티브 | 네이티브 |
| 체크포인트 | 없음 | 내장 지원 |
| 메트릭 관리 | ResultKey로 버전 관리 | ValidationDefinition으로 관리 |

## 프로젝트 구조

```
enjoy-workreduce/
├── src/
│   ├── mmix/
│   │   ├── __init__.py
│   │   └── common/
│   │       ├── __init__.py
│   │       └── utils.py              # 공통 유틸리티 함수
│   ├── example_spark_deequ.py        # PyDeequ 합성 데이터 예제
│   └── example_spark_mysql.py        # MySQL 연동 PyDeequ 예제
├── notebooks/
│   ├── emr/                         # EMR 환경 예제
│   ├── flink/                       # Apache Flink 스트리밍 예제
│   └── spark/                        # Spark 클러스터 환경 예제
│       └── great_expectations/       # Great Expectations 설정
├── tests/                           # 테스트 코드
├── dist/                            # 빌드 결과물
├── pyproject.toml                   # 프로젝트 메타데이터
├── requirements.txt                 # Python 의존성
├── pytest.ini                       # pytest 설정
├── conftest.py                      # pytest fixtures
└── README.md                        # 이 파일
```

## 문제 해결

### Spark 세션이 시작되지 않는 경우

1. Java가 설치되어 있는지 확인:
   ```bash
   java -version
   ```

2. JAVA_HOME 환경 변수 설정:
   ```bash
   export JAVA_HOME=/path/to/java
   ```

### MySQL JDBC 연결 오류

1. JDBC 드라이버 JAR 파일 경로 확인
2. `--jars` 옵션에 올바른 경로 지정
3. MySQL 호스트 및 포트 접근 가능 여부 확인

### PyDeequ 분석 실패

1. PySpark 버전과 PyDeequ 버전 호환성 확인
2. Deequ JAR 파일이 PySpark jars 디렉토리에 있는지 확인
3. Spark 세션 생성 시 메모리 설정 확인

### Great Expectations CLI 실행 오류

Great Expectations 패키지는 설치되어 있지만 CLI 실행 파일이 PATH에 없는 경우 발생합니다.

#### 현상

```bash
great_expectations --version
# zsh: command not found: great_expectations
```

#### 원인

conda 환경에서 Great Expectations가 설치되었지만 CLI 실행 파일이 PATH에 등록되지 않은 상태입니다.

#### 해결책 A: python -m 방식 사용 (권장)

CLI가 PATH에 없어도 이 방식은 항상 동작합니다. 운영 환경에서도 이 방식이 가장 안전합니다.

```bash
python -m great_expectations --version
python -m great_expectations init
```

#### 해결책 B: 현재 conda env에 설치 확인

1. 환경 활성화 확인:
   ```bash
   conda activate enjoy-workreduce
   ```

2. 설치 확인:
   ```bash
   pip show great-expectations
   ```

3. 설치되지 않았다면:
   ```bash
   pip install great-expectations==1.11.0
   ```

#### 해결책 C: PATH 문제 해결 (macOS + conda에서 자주 발생)

실행 파일이 conda 환경 bin 디렉토리에 있는지 확인:

```bash
ls /opt/anaconda3/envs/enjoy-workreduce/bin | grep great
```

있다면 직접 실행:

```bash
/opt/anaconda3/envs/enjoy-workreduce/bin/great_expectations --version
```

또는 PATH에 추가:

```bash
export PATH="/opt/anaconda3/envs/enjoy-workreduce/bin:$PATH"
```

#### Jupyter Notebook에서 실행 시 주의사항

Notebook은 쉘 환경과 다를 수 있습니다. Notebook 셀에서 확인:

```python
!which python
!python -m great_expectations --version
```

Notebook 커널이 다른 환경이라면:

```bash
python -m ipykernel install --user --name enjoy-workreduce
```

이후 Jupyter에서 커널을 `enjoy-workreduce`로 변경합니다.

#### 방법별 권장도

| 방법 | 권장도 | 비고 |
|------|--------|------|
| `python -m great_expectations` | 강력 권장 | 운영 환경, Airflow, Spark job에서 사용 |
| PATH 설정 후 `great_expectations` | 권장 | 개발 환경에서 편리 |
| Jupyter에서 `!great_expectations` | 보통 | 커널 환경 확인 필요 |

## 라이선스

MIT License

## 기여자

- Genius (csj4032@gmail.com)



