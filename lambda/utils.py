import boto3
from botocore.exceptions import ClientError
from datetime import datetime

DEFAULT_TABLE = "devcraft_admin_table3"

MODEL_ARN_MAP = {
    "Claude 3 Haiku": "arn:aws:bedrock:ap-south-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0",
    "Claude 3 Sonnet": "arn:aws:bedrock:ap-south-1::foundation-model/anthropic.claude-3-sonnet-20240229-v1:0",
    "Claude 3.5 Sonnet (Cross-region inference)": "apac.anthropic.claude-3-5-sonnet-20240620-v1:0",
    "Claude 3.5 Sonnet v2 (Cross-region inference)": "apac.anthropic.claude-3-5-sonnet-20241022-v2:0",
    "Claude 3.7 Sonnet (Cross-region inference)": "apac.anthropic.claude-3-7-sonnet-20250219-v1:0",
    "Claude Sonnet 4 (Cross-region inference)": "apac.anthropic.claude-sonnet-4-20250514-v1:0",
    "Nova Pro (Cross-region inference)": "apac.amazon.nova-pro-v1:0",
    "Nova Lite (Cross-region inference)": "apac.amazon.nova-lite-v1:0",
    "Nova Micro (Cross-region inference)": "apac.amazon.nova-micro-v1:0"
}

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

def current_timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def ensure_table_exists(table_name):
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'id', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
        else:
            raise
