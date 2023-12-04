# README.md for #lakeH Terraform and AWS Glue Script

## Overview
This README provides an overview of the Terraform configuration and AWS Glue scripts used for the #lakeH project. It includes setup for AWS resources, step functions, SNS topics, email subscriptions, and AWS Glue jobs for data processing.

## Terraform Configuration
Terraform is used for setting up AWS resources. The main components are:
- **AWS Provider Configuration**: Set up with a specific AWS profile and region (us-east-1).
- **AWS CloudWatch Log Group**: For logging state machine executions.
- **AWS Step Function (State Machine)**: Defined for various data processing tasks. It includes error logging and email notifications on failures.
- **AWS SNS Topics and Subscriptions**: For sending email alerts on specific errors or issues detected during the execution of the state machine.
- **Variables**: Email addresses for subscriptions are declared as variables.

## AWS Glue Scripts
AWS Glue scripts are provided for data processing tasks. These scripts include:
- Reading and writing data to AWS S3 in parquet format.
- Data transformations using PySpark.
- Error handling and logging.
- Use of AWS Glue specific functions for data processing.
- Daily data processing jobs fetching the previous day's data.
- Data renaming and schema adjustments as per the requirements.

## Step Functions Definition
The step function includes tasks such as:
- Parallel processing of different data sets.
- Lambda function invocations for data checks.
- SNS notifications for error handling.
- Choices and conditions for data availability checks.
- Waiting periods between retries.
- Success and failure handlers.

## AWS Glue Jobs
Several AWS Glue jobs are defined for various datasets like 'activacionesdiarias', 'facturacioncomercial', 'libreutilizacion', etc. These jobs include:
- Reading data from specific S3 paths.
- Data transformations and renaming of columns.
- Writing transformed data back to S3 in a different location.

## Usage
To use these scripts:
1. **Terraform Initialization**: Run `terraform init` to initialize Terraform.
2. **Terraform Plan & Apply**: Execute `terraform plan` and `terraform apply` to create the AWS resources.
3. **Glue Scripts**: Deploy the AWS Glue scripts in the AWS Glue console or use AWS CLI.
4. **Monitoring and Logs**: Monitor the execution logs in AWS CloudWatch and track the execution of the step function in the AWS Management Console.

