resource "aws_emr_cluster" "this" {
  count                = var.enable_emr ? 1 : 0
  name                 = "${var.project_name}-emr"
  release_label        = var.emr_release_label
  applications         = ["Spark", "Hadoop"]
  service_role         = aws_iam_role.emr_service_role.name
  ec2_attributes {
    instance_profile = aws_iam_instance_profile.emr_ec2_profile.name
  }
  master_instance_group {
    instance_type = var.emr_master_instance_type
    instance_count = 1
  }
  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count
  }
  log_uri = "s3n://${aws_s3_bucket.data_lake.bucket}/logs/"
  tags    = local.common_tags
}

data "aws_iam_policy_document" "emr_ec2_assume" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "emr_ec2_role" {
  name               = "${var.project_name}-emr-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "emr_ec2_managed" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_ec2_profile" {
  name = "${var.project_name}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2_role.name
}


