variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = null
}

variable "project_name" {
  description = "Project name used for tagging and naming"
  type        = string
  default     = "financial-data-pipeline"
}

variable "data_lake_bucket" {
  description = "Primary S3 bucket for data lake"
  type        = string
  default     = "financial-data-lake-demo"
}

variable "processed_bucket" {
  description = "S3 bucket for processed outputs/checkpoints"
  type        = string
  default     = "financial-data-processed-demo"
}

variable "enable_emr" {
  description = "Whether to provision an EMR cluster (costs may apply)"
  type        = bool
  default     = false
}

variable "emr_release_label" {
  description = "EMR release label"
  type        = string
  default     = "emr-6.15.0"
}

variable "emr_master_instance_type" {
  description = "EMR master instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "EMR core instance type"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_count" {
  description = "EMR core instance count"
  type        = number
  default     = 2
}

variable "state_bucket_name" {
  description = "Optional S3 bucket for Terraform remote state"
  type        = string
  default     = ""
}


