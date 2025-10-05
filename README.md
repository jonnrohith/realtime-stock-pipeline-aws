# Yahoo Finance Data Pipeline (Kafka → PySpark → S3 → dbt)

An end-to-end data engineering project focused only on Yahoo Finance data. It supports both batch and streaming, lands data in an S3 data lake, builds analytics models with dbt, and is cloud-ready with Terraform and GitHub Actions.

## Highlights
- Yahoo-only ingestion: tickers, quotes, news, history
- Realtime (Kafka + PySpark Structured Streaming) and batch
- S3/MinIO data lake in Parquet/JSON
- Optional EMR cluster for managed Spark
- dbt marts for analytics; Airflow DAG example
- Terraform IaC and conditional CI (validate/plan/apply) via OIDC

## Architecture
```
Yahoo Finance → Kafka → PySpark (batch/stream) → S3/MinIO → dbt marts
                                 │                       │
                                 └──── Airflow (DAGs) ───┘
```

## Repository Structure
```
streaming_pipeline/   # Kafka producer and PySpark streaming job
data_pipeline/        # extractors, transformers, models, orchestration helpers
data_lake/            # S3/MinIO integration utilities
dbt_models/           # dbt project (staging + marts)
infra/terraform/      # Terraform (S3, IAM, optional EMR)
orchestration/        # Airflow example DAG
visualization/        # Optional Streamlit dashboard
```

## Local Setup
1) Install
```bash
pip install -r requirements.txt
cp env.example .env            # or: cp env.aws.example .env
```
2) Run examples
```bash
python stocks_example.py       # Yahoo extraction sample
python pipeline_example.py     # End-to-end sample pipeline
```
Outputs: `data/processed/`, `spark_output/`.

## Streaming
- Producer: `streaming_pipeline/kafka_producer.py` (push Yahoo events into Kafka)
- Consumer/Processor: `streaming_pipeline/pyspark_streaming.py` (Structured Streaming → S3)

Run locally with a Kafka container (see `docker-compose.working.yml`) or point at AWS MSK.

## S3 Data Lake
- Core utility: `data_lake/s3_storage.py`
- Supports Parquet and JSON with partitioned paths and reads
- Works with MinIO locally; AWS S3 in cloud

## Analytics with dbt
```bash
cd dbt_models
# Update profiles.yml if needed, then:
dbt deps
+ dbt run
+ dbt test
```
Models: `staging/*`, `marts/*` (e.g., `fact_stock_quotes.sql`, `dim_stocks.sql`).

## Cloud Deployment (AWS)
Terraform IaC in `infra/terraform/` provisions:
- S3 data lake buckets
- IAM roles and policies
- Optional EMR cluster (toggle `enable_emr`)

1) Configure tfvars
```bash
cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars
# Edit: data_lake_bucket, processed_bucket, aws_region, enable_emr=false
```
2) Deploy
```bash
./deploy-to-aws.sh  # set AUTO_APPLY=true to auto-apply
```
3) Upload jobs (script does this if aws cli available):
- `streaming_pipeline/pyspark_streaming.py` → `s3://<data-lake-bucket>/spark-jobs/`
- `data_pipeline/` → `s3://<data-lake-bucket>/data-pipeline/`

### EMR (optional)
- Set `enable_emr=true` in `infra/terraform/terraform.tfvars`
- Apply Terraform, then:
```bash
CLUSTER_ID=$(terraform -chdir=infra/terraform output -raw emr_cluster_id)
./emr/submit_emr_steps.sh "$CLUSTER_ID" s3://<data-lake-bucket>/spark-jobs/pyspark_streaming.py
```

## CI/CD (GitHub Actions)
Workflow: `.github/workflows/terraform.yml`
- Always: `terraform init -backend=false` + `terraform validate`
- If secret present: assumes OIDC role and runs plan/apply

Setup:
- Create IAM OIDC provider: `https://token.actions.githubusercontent.com` (audience `sts.amazonaws.com`)
- Create role with trust condition:
  - `token.actions.githubusercontent.com:sub = repo:jonnrohith/realtime-stock-pipeline-aws:ref:refs/heads/main`
- Add repo secret `AWS_TERRAFORM_ROLE_ARN` with that role ARN
- Optional repo variable `TERRAFORM_AUTO_APPLY=true`

## Notable Files
- `yahoo_client.py`, `updated_yahoo_client.py` – Yahoo data clients
- `stocks_extractor.py`, `stocks_example.py` – Yahoo extractors and examples
- `streaming_pipeline/pyspark_streaming.py` – streaming job
- `data_lake/s3_storage.py` – S3 read/write helpers
- `orchestration/airflow_dag.py` – example DAG

## Troubleshooting
- OIDC assume error: verify provider URL, trust `sub` (refs/heads/main), and secret ARN
- S3 errors: confirm bucket names/region and credentials; for local, use MinIO env in examples
- EMR steps failing: check `log_uri` in EMR and job S3 paths

## License
MIT

