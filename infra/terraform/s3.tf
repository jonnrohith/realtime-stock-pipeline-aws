locals {
  common_tags = {
    Project     = var.project_name
    Environment = "demo"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket" "data_lake" {
  bucket = var.data_lake_bucket
  tags   = local.common_tags
}

resource "aws_s3_bucket" "processed" {
  bucket = var.processed_bucket
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}


