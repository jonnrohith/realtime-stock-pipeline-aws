data "aws_iam_policy_document" "emr_s3_access" {
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.data_lake.arn,
      aws_s3_bucket.processed.arn
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.data_lake.arn}/*",
      "${aws_s3_bucket.processed.arn}/*"
    ]
  }
}

resource "aws_iam_role" "emr_service_role" {
  name               = "${var.project_name}-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_assume_role.json
}

data "aws_iam_policy_document" "emr_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_policy" "emr_s3_policy" {
  name   = "${var.project_name}-emr-s3-policy"
  policy = data.aws_iam_policy_document.emr_s3_access.json
}

resource "aws_iam_role_policy_attachment" "emr_s3_attach" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = aws_iam_policy.emr_s3_policy.arn
}


