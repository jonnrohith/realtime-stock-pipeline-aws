terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# Optional S3 backend example (uncomment and configure to use remote state)
# backend "s3" {
#   bucket = "${var.state_bucket_name}"
#   key    = "terraform/state/financial-data-pipeline.tfstate"
#   region = var.aws_region
#   profile = var.aws_profile
#   encrypt = true
# }


