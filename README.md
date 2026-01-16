# enjoy-workreduce

## Build

To build the project, run the following command:

### Initial Setup

```bash
pip install build
pip install --upgrade --force-reinstall setuptools
```

### Build the Package

```bash
python -m build
```

## Conda Environment

```bash
conda create -n enjoy-workreduce python==3.10.12

conda activate enjoy-workreduce
```
## Requirement

```bash
pip install -r requirements.txt
```

## Install Jars

```bash
cd /opt/anaconda3/envs/enjoy-workreduce/lib/python3.10/site-packages/pyspark/jars/
cd /opt/miniconda3/envs/enjoy-workreduce/lib/python3.10/site-packages/pyspark/jars/
```

## Install Package
```bash
pip install sparkmagic emr-serverless-customauth
```

## S3 Copy Local to S3

```bash
aws s3 cp ./src s3://mmix-prod-dataengineer-workreduce/src --recursive --profile mmix-genius
aws s3 cp ./dist s3://mmix-prod-dataengineer-workreduce/dist --recursive --profile mmix-genius
```

## S3 Copy Local to S3 (Minio)

```bash
aws s3 cp ./src s3://mmix-prod-dataengineer-workreduce/src --recursive --endpoint-url http://minio:9000 --profile minio
```