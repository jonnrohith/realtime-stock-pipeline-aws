#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "[1/3] Terraform init/plan/apply"
pushd "$ROOT_DIR/infra/terraform" >/dev/null
terraform init -input=false
terraform plan -input=false -out=tfplan
if [[ "${AUTO_APPLY:-false}" == "true" ]]; then
  terraform apply -auto-approve tfplan
else
  echo "Skipping apply (set AUTO_APPLY=true to auto-apply)"
fi
popd >/dev/null

echo "[2/3] Sync local jobs to S3 (if AWS CLI configured)"
if command -v aws >/dev/null 2>&1; then
  DATA_LAKE_BUCKET=$(terraform -chdir="$ROOT_DIR/infra/terraform" output -raw data_lake_bucket || true)
  if [[ -n "$DATA_LAKE_BUCKET" ]]; then
    aws s3 sync "$ROOT_DIR/streaming_pipeline" "s3://$DATA_LAKE_BUCKET/spark-jobs/" --exclude "*" --include "*.py"
    aws s3 sync "$ROOT_DIR/data_pipeline" "s3://$DATA_LAKE_BUCKET/data-pipeline/"
  fi
else
  echo "aws CLI not found; skipping S3 sync"
fi

echo "[3/3] Done. Configure GitHub OIDC and set AWS_TERRAFORM_ROLE_ARN for CI."


