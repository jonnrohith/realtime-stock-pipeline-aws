#!/usr/bin/env bash
set -euo pipefail

CLUSTER_ID=${1:-}
JOB_S3_PATH=${2:-s3://financial-data-lake-demo/spark-jobs/pyspark_streaming.py}

if [[ -z "$CLUSTER_ID" ]]; then
  echo "Usage: $0 <emr-cluster-id> [s3://path/to/job.py]"
  exit 1
fi

aws emr add-steps \
  --cluster-id "$CLUSTER_ID" \
  --steps Type=Spark,Name="StreamingJob",ActionOnFailure=CONTINUE,Args=["$JOB_S3_PATH"]

echo "Submitted step to EMR cluster $CLUSTER_ID"


