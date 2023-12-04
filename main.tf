#lakeH SV
provider "aws" {
  alias = "dev"
  profile = "294826912937_AWSAdministratorAccess" 
  region  = "us-east-1"
}

resource "aws_cloudwatch_log_group" "state_machine_logs_dev" {
  provider = aws.dev
  name = "/aws/states/state-machine-4-pl-devices-sv"
}

# module "step_function" {
#   source = "terraform-aws-modules/step-functions/aws"

#   providers = {
#     aws = aws.dev
#   }

#   name       = "stp-sv-4-pl-devices-dev"
#   definition = file("current_definition.json")
#   type       = "STANDARD"
#   role_arn   = "arn:aws:iam::294826912937:role/data-service-rol-sv-dev-glue-4pl-devices"

#   logging_configuration = {
#     log_destination = "${aws_cloudwatch_log_group.state_machine_logs_dev.arn}:*"
#     level            = "ERROR"
#   }
# }

resource "aws_sfn_state_machine" "stp-sv-4-pl-devices-dev" {
  provider = aws.dev
  name     = "stp-sv-4-pl-devices-dev"
  role_arn = "arn:aws:iam::294826912937:role/data-service-rol-sv-dev-glue-4pl-devices"
  
  definition = file("current_definition.json")

  logging_configuration {
    log_destination = "${aws_cloudwatch_log_group.state_machine_logs_dev.arn}:*"
    level           = "ERROR"
    include_execution_data = true
  }
}


resource "aws_sns_topic" "logging_error_topic_dev" {
  provider = aws.dev
  name = "4-pl-devices-sv-stf-logging-error-dev"
}

# # Lista de correos electrónicos
variable "emails" {
  description = "Lista de correos electrónicos para suscripción."
  type        = list(string)
  default     = [
    "simon.bustamante@millicom.com", 
    #"email2@gmail.com", 
  ]
}

resource "aws_sns_topic_subscription" "elogging_error_subscription_dev" {
  provider = aws.dev
  for_each  = toset(var.emails)

  topic_arn = aws_sns_topic.logging_error_topic_dev.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_sns_topic" "error_table_not_found_dev" {
  provider = aws.dev
  name = "4-pl-devices-sv-table-not-found-dev"
  display_name = "4-pl-devices-sv-table-not-found-dev"
}

resource "aws_sns_topic_subscription" "eError_table_not_found_dev" {
  provider = aws.dev
  for_each  = toset(var.emails)

  topic_arn = aws_sns_topic.error_table_not_found_dev.arn
  protocol  = "email"
  endpoint  = each.value
}

# resource "aws_cloudwatch_log_metric_filter" "logging_error_filter_dev" {
#   provider = aws.dev
#   name           = "4-pl-devices-sv-stf-logging-error-dev-filter"
#   pattern        = "{ $.type = \"TaskFailed\" || $.type = \"ExecutionFailed\" }"
#   log_group_name = aws_cloudwatch_log_group.state_machine_logs_dev.name

#   metric_transformation {
#     name      = "StepFunctionFailed"
#     namespace = "4pldevices/States"
#     value     = "1"
#   }
# }

# resource "aws_cloudwatch_metric_alarm" "logging_error_alarm_dev" {
#   provider = aws.dev
#   alarm_name          = "state-machine-failure-alarm"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = "1"
#   metric_name         = "stepFunctionFailed"
#   namespace           = "4pldevices/States"
#   period              = "60"
#   statistic           = "Sum"
#   threshold           = "1"
#   alarm_description   = "state machine failed"
#   alarm_actions       = [aws_sns_topic.logging_error_topic_dev.arn]
# }