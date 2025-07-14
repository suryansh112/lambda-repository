import boto3
from boto3.dynamodb.conditions import Key
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_parameter_value(parameter_name, environment=None):
    table_name = os.environ.get('TableName')
    if not table_name:
        logger.error("Environment variable 'TableName' is not set.")
        return None

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    logger.info(f"Querying parameter value for: {parameter_name} from table: {table_name}")

    try:
        # Query the table
        response = table.query(
            KeyConditionExpression=Key('ParameterName').eq(parameter_name)
        )

        items = response.get('Items', [])

        # Optional filter on ParameterEnvironment if provided
        if environment:
            items = [item for item in items if item.get('ParameterEnvironment') == environment]

        if items:
            value = items[0].get('ParameterValue')
            logger.info(f"Found value for '{parameter_name}' in env '{environment}': {value}")
            return value
        else:
            logger.warning(f"No matching item found for ParameterName: {parameter_name} and env: {environment}")
            return None

    except Exception as e:
        logger.error(f"Error querying parameter '{parameter_name}': {e}")
        return None
