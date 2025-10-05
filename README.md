# AWS Financial Data Pipeline (Yahoo → Kafka → Spark → S3 → dbt)

[![CI](https://github.com/jonnrohith/realtime-stock-pipeline-aws/actions/workflows/terraform.yml/badge.svg)](https://github.com/jonnrohith/realtime-stock-pipeline-aws/actions/workflows/terraform.yml)
![Cloud](https://img.shields.io/badge/Cloud-AWS-orange)
![Infra](https://img.shields.io/badge/IaC-Terraform-7B42BC)

An end-to-end, production-ready data engineering pipeline for real-time and batch stock market data using Yahoo Finance. The system streams via Kafka, processes with PySpark, lands data in an S3 data lake, models analytics with dbt, and optionally runs Spark on EMR. Terraform IaC lives under `infra/terraform` and CI is GitHub Actions with OIDC.

![Architecture](docs/architecture-diagram.svg)

## Architecture Overview

```
Yahoo Finance → Kafka → PySpark (batch/stream) → S3/MinIO → dbt marts
                                 │                       │
                                 └──── Airflow (DAGs) ───┘
```

## Technology Stack

- Ingestion: Yahoo Finance APIs (quotes, news, history)
- Streaming: Apache Kafka; Python producers
- Processing: Apache Spark / PySpark Structured Streaming
- Storage: S3/MinIO data lake (Parquet/JSON)
- Analytics: dbt (staging + marts)
- Orchestration: Airflow
- Monitoring: basic metrics/logs; optional Grafana/CloudWatch
- Infra: Terraform; optional EMR cluster

## Project Structure

```
data_pipeline/        # core pipeline (extract/transform/models/orchestration helpers)
streaming_pipeline/   # Kafka producer and PySpark streaming job
data_lake/            # S3/MinIO integration
dbt_models/           # dbt project (staging + marts)
infra/terraform/      # Terraform (S3, IAM, optional EMR)
visualization/        # Streamlit dashboard
orchestration/        # Airflow DAG
```

## Quick Start (Local)

1) Install
```bash
pip install -r requirements.txt
cp env.example .env        # or: cp env.aws.example .env
```

2) Run examples
```bash
python stocks_example.py            # simple Yahoo extraction
python pipeline_example.py          # end-to-end sample pipeline
```

Outputs appear under `data/processed/` and `spark_output/`.

## Cloud Deployment (Showcase)

1) Configure Terraform vars in `infra/terraform/terraform.tfvars` (bucket names, region; enable_emr=false to start).

2) Deploy
```bash
./deploy-to-aws.sh  # set AUTO_APPLY=true to auto-apply
```

3) CI (GitHub Actions)
- Add secret `AWS_TERRAFORM_ROLE_ARN` (OIDC role ARN)
- Optional repo variable `TERRAFORM_AUTO_APPLY=true` for auto-apply on main

4) Optional EMR
- Set `enable_emr=true` in tfvars and apply
- Submit a step:
```bash
CLUSTER_ID=$(terraform -chdir=infra/terraform output -raw emr_cluster_id)
./emr/submit_emr_steps.sh "$CLUSTER_ID" s3://<data-lake-bucket>/spark-jobs/pyspark_streaming.py
```

## dbt

```bash
cd dbt_models
dbt deps && dbt run && dbt test
```

## Notable Files

- `yahoo_client.py`, `updated_yahoo_client.py`: Yahoo data clients
- `stocks_extractor.py`, `stocks_example.py`: extraction utilities and examples
- `data_lake/s3_storage.py`: S3 read/write helpers (Parquet/JSON)
- `streaming_pipeline/pyspark_streaming.py`: Spark streaming job
- `orchestration/airflow_dag.py`: example DAG wiring extract → transform → load-to-S3 → dbt
- `.github/workflows/terraform.yml`: CI validate/plan (conditional) and optional apply

## Troubleshooting

- CI cannot assume role: verify IAM OIDC provider URL is `https://token.actions.githubusercontent.com`, trust `sub` matches `repo:jonnrohith/realtime-stock-pipeline-aws:ref:refs/heads/main`, and secret `AWS_TERRAFORM_ROLE_ARN` points to that role.
- S3 access issues: check bucket names in tfvars and your AWS credentials locally if running `deploy-to-aws.sh`.

## License

MIT

# AWS Financial Data Pipeline (Yahoo → Kafka → Spark → S3 → dbt)

[![CI](https://github.com/jonnrohith/realtime-stock-pipeline-aws/actions/workflows/terraform.yml/badge.svg)](https://github.com/jonnrohith/realtime-stock-pipeline-aws/actions/workflows/terraform.yml)
![Cloud](https://img.shields.io/badge/Cloud-AWS-orange)
![Infra](https://img.shields.io/badge/IaC-Terraform-7B42BC)

An end-to-end, cloud-ready pipeline for real-time and batch stock market data from Yahoo Finance. Stream with Kafka, process with PySpark, store in S3, transform with dbt, and optionally run on EMR. Terraform IaC is under `infra/terraform`; CI is GitHub Actions.

See `AWS_DEPLOYMENT_GUIDE.md` for deployment steps.

![Architecture](docs/architecture-diagram.svg)

## Features

- Market data ingestion (tickers, quotes, news, history)
- PySpark batch/stream processing; Kafka for realtime ingest
- S3 data lake (Parquet/JSON); MinIO for local dev
- dbt analytics marts; optional EMR cluster
- CI with GitHub Actions; IaC with Terraform

## Install

```bash
pip install -r requirements.txt
cp env.example .env  # or cp env.aws.example .env
```

## Quick Start

```bash
python stocks_example.py
python pipeline_example.py
```

Outputs are written to `data/processed/` and `spark_output/`.

## Cloud

```bash
./deploy-to-aws.sh  # set AUTO_APPLY=true to auto-apply
```
Then follow `AWS_DEPLOYMENT_GUIDE.md` to enable EMR and CI plan/apply via OIDC.

## Structure

- `yahoo_client.py`, `updated_yahoo_client.py`
- `stocks_extractor.py`, `stocks_example.py`
- `data_lake/s3_storage.py`
- `data_pipeline/`
- `dbt_models/`
- `infra/terraform/`

## License

MIT
