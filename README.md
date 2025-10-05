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
