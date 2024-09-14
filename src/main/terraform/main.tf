# main.tf

provider "aws" {
  version = "~> 2.16"
  region  = "us-east-1"
}

provider "null" {
  version = "~> 2.1.2"
}

locals {
  region        = "us-east-1"
  account_id    = "125100207114"
  is_production = local.env == "production"
}

#################################################
# Set these to appropriate values for your lambda
locals {
  app_name           = "model_d"
  app_display_name   = "Demand model D"
  lambda_description = "Predicts recurring meals using Kaplan-Meier survival curve"
  lambda_handler     = "lambda_model_d.lambda_handler"
  snowflake_db       = local.env == "production" ? "freshly" : "freshly_dev"
  data_s3_bucket     = local.env == "production" ? "freshly-data-production" : "freshly-data-dev"

  # Maximum time (seconds) to allow the lambda to run -- max 900
  lambda_timeout = 900

  # Maximum memory (MB) the lambda can use -- max 3008
  # See https://docs.aws.amazon.com/lambda/latest/dg/limits.html
  memory_size = 1024

  # See https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html#CronExpressions
  # Set this for a time when Snowflake is likely to have replicated data for the previous hour
  hourly_schedule_expr = "cron(35 * * * ? *)"
  weekly_schedule_expr = "cron(47 3 ? * SUN *)"
}

#################################################

locals {
  lambda_function_name = "${local.app_name}-${local.env}"

  target_path = "../../../target"

  # Use the same deployment package regardless of environment
  # Must match PACKAGE_NAME in ./Dockerfile
  lambda_deployment_package = "${local.target_path}/lambda-model-d.zip"

  s3_folder = "lambda-deploy/${local.app_name}/${local.env}"
  s3_file   = "lambda-model-d-${var.app_version}.zip"
  s3_key    = "${local.s3_folder}/${local.s3_file}"
}

###########################################################
# Create an IAM policy allowing the Lambda to invoke itself

data "aws_iam_policy_document" "lambda_policy_doc" {
  statement {
    actions = ["lambda:InvokeFunction"]

    # There is a circular dependency here -- the role policy refers to the lambda,
    # but the lambda refers to the role.  I think this causes Terraform to think
    # the policy has changed even when it hasn't.
    #resources = [aws_lambda_function.main.arn]
    resources = ["*"]
  }
}

resource "aws_iam_role" "lambda" {
  name               = local.lambda_function_name
  assume_role_policy = data.terraform_remote_state.iam.outputs.lambda_assume_role_policy_json

  tags = merge(
    local.common_tags,
    {
      "Name" = "lambda-${local.app_name}-${local.env}"
    }
  )
}

# Attach the policy to the role
resource "aws_iam_role_policy" "lambda_policy" {
  name   = local.lambda_function_name
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda_policy_doc.json
}

# Attach policy allowing the lambda to access Snowflake creds
resource "aws_iam_role_policy_attachment" "lambda_snowflake" {
  role       = aws_iam_role.lambda.name
  policy_arn = data.terraform_remote_state.iam.outputs.snowflake_model_user_policy_arn
}

# Attach policy allowing the lambda to publish alerts to SNS
resource "aws_iam_role_policy_attachment" "lambda_sns_alerts" {
  role       = aws_iam_role.lambda.name
  policy_arn = data.terraform_remote_state.iam.outputs.sns_alerts_policy_arn
}

# Also attach the AWS managed Lambda policy to the role
resource "aws_iam_role_policy_attachment" "lambda_execution_policy" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

############################
# Create the Lambda function

# Upload the deployment package to S3 since it is larger than 50MB
# TODO: Use a dedicated bucket?
data "aws_s3_bucket" "deploy" {
  bucket = "freshly-data-engineering-logs-us-east-1"
}

# Copy the file to S3 if it doesn't already exist
# aws_s3_bucket_object would be nicer, but that won't preserve existing versions -- if we upload version 1,
# and then run again later with version 2, it will delete version 1 instead of just adding version 2
resource "null_resource" "deploy_to_s3" {
  triggers = {
    bucket = data.aws_s3_bucket.deploy.id
    key    = local.s3_key
  }

  provisioner "local-exec" {
    command = "(cp -n ${local.lambda_deployment_package} ${local.target_path}/${local.s3_file} || true) && aws s3 sync ${local.target_path} s3://${data.aws_s3_bucket.deploy.id}/${local.s3_folder} --exclude '*' --include '${local.s3_file}' --no-progress"
  }
}

resource "aws_lambda_function" "main" {
  function_name    = local.lambda_function_name
  description      = local.lambda_description
  runtime          = "python3.7"
  s3_bucket        = data.aws_s3_bucket.deploy.id
  s3_key           = local.s3_key
  source_code_hash = filebase64sha256(local.lambda_deployment_package)
  handler          = local.lambda_handler
  timeout          = local.lambda_timeout
  memory_size      = local.memory_size
  role             = aws_iam_role.lambda.arn

  environment {
    variables = {
      env                 = local.env
      aws_region          = local.region
      snowflake_db        = local.snowflake_db
      snowflake_secret_id = data.terraform_remote_state.iam.outputs.snowflake_lambda_secret_id
      s3_path             = "${local.data_s3_bucket}/model-d/"
      sns_topic_arn       = data.terraform_remote_state.monitoring.outputs.aws_sns_topic_arn
      lambda_name         = local.lambda_function_name
      lambda_version      = var.app_version
    }
  }

  tags = merge(
    local.common_tags,
    {
      "Name"    = local.lambda_description
      "Version" = var.app_version
    }
  )

  depends_on = [null_resource.deploy_to_s3]
}

resource "aws_cloudwatch_log_group" "main" {
  name              = "/aws/lambda/${local.lambda_function_name}"
  retention_in_days = 7

  tags = merge(
    local.common_tags,
    {
      "Name" = local.app_display_name
    }
  )
}

################################################################
# Set up CloudWatch events to trigger the lambda weekly & hourly
resource "aws_cloudwatch_event_rule" "weekly" {
  count = local.env == "production" ? 1 : 0

  name                = "${local.lambda_function_name}-weekly"
  description         = "Demand model D - weekly training"
  schedule_expression = local.weekly_schedule_expr
}

resource "aws_cloudwatch_event_target" "weekly" {
  count = local.env == "production" ? 1 : 0

  target_id = local.lambda_function_name
  rule      = aws_cloudwatch_event_rule.weekly[count.index].name
  arn       = aws_lambda_function.main.arn
  input     = "{\"mode\": \"weekly\"}"
}

resource "aws_lambda_permission" "weekly" {
  count = local.env == "production" ? 1 : 0

  statement_id  = "AllowExecutionFromCloudWatchEventWeekly"
  action        = "lambda:InvokeFunction"
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weekly[count.index].arn
  function_name = aws_lambda_function.main.arn
}

resource "aws_cloudwatch_event_rule" "hourly" {
  count = local.env == "production" ? 1 : 0

  name                = "${local.lambda_function_name}-hourly"
  description         = "Demand model D - hourly correction"
  schedule_expression = local.hourly_schedule_expr
}

resource "aws_cloudwatch_event_target" "hourly" {
  count = local.env == "production" ? 1 : 0

  target_id = local.lambda_function_name
  rule      = aws_cloudwatch_event_rule.hourly[count.index].name
  arn       = aws_lambda_function.main.arn
  input     = "{\"mode\": \"hourly\"}"
}

resource "aws_lambda_permission" "hourly" {
  count = local.env == "production" ? 1 : 0

  statement_id  = "AllowExecutionFromCloudWatchEventHourly"
  action        = "lambda:InvokeFunction"
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hourly[count.index].arn
  function_name = aws_lambda_function.main.arn
}

#######################################
# Set up CloudWatch alarms if necessary
# TODO

