output "data_lake_bucket" {
  value       = aws_s3_bucket.data_lake.bucket
  description = "Name of the data lake bucket"
}

output "processed_bucket" {
  value       = aws_s3_bucket.processed.bucket
  description = "Name of the processed bucket"
}

output "emr_cluster_id" {
  value       = try(aws_emr_cluster.this[0].id, null)
  description = "EMR cluster id (if enabled)"
}


