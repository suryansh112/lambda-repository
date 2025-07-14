import boto3
import copy
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key
import json


# Admin table to store the details
table_name_1 = "devcraft_admin_table3"
dynamodb = boto3.resource("dynamodb")

def get_id_details(action):
    table = dynamodb.Table(table_name_1)
    if action in ["rag_delete", "rag_create"]:
        item_id = "1"
    elif action in ["text2SQL_delete", "text2SQL_create"]:
        item_id = "2"
    elif action in ["ocr_delete", "ocr_create"]:
        item_id = "3"
    else:
        return {"error": "Invalid action provided."}

    try:
        result = table.get_item(Key={"id": item_id})
        if "Item" in result:
            return result["Item"]
        else:
            return {"error": f"No record found with id: {item_id}"}
    except Exception as e:
        return {"error": f"Error fetching record: {str(e)}"}

def update_status(item):
    table = dynamodb.Table(table_name_1)
    response = table.update_item(
        Key={"id": item["id"]},
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": item["status"]},
        ReturnValues="UPDATED_NEW"
    )
    return response






# Table to store the stack delete status timestamp
stack_table_name = "devcraft_admin_stack_status_table"
DEFAULT_REGION = "ap-south-1"

dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

def current_timestamp():
    """Generate current timestamp in IST format"""
    ist_time = datetime.utcnow() + timedelta(hours=5, minutes=30)
    return ist_time.strftime("%d-%m-%Y-%H-%M-%S")

def ensure_stack_table_exists(table_name):
    """Ensure DynamoDB table exists, create if not"""
    try:
        dynamodb_client.describe_table(TableName=table_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            dynamodb_client.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'stack_id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'stack_id', 'AttributeType': 'S'}
                ],
                BillingMode='PAY_PER_REQUEST'
            )
            waiter = dynamodb_client.get_waiter('table_exists')
            waiter.wait(TableName=table_name)
        else:
            raise


def store_stack_deletion_timestamp(action, table_name=None, region=None):
    """
    Store timestamp for stack deletion action
    
    Args:
        action (str): The deletion action ('rag_delete', 'text2SQL_delete', 'ocr_delete')
        table_name (str, optional): DynamoDB table name
        region (str, optional): AWS region
    
    Returns:
        dict: Response with status, saved_id, and timestamp
    """
    if table_name is None:
        table_name = stack_table_name
    if region is None:
        region = DEFAULT_REGION
    
    # Ensure table exists
    ensure_stack_table_exists(table_name)
    
    # Map action to item_id
    action_mapping = {
        "rag_delete": "1",
        "text2SQL_delete": "2", 
        "ocr_delete": "3"
    }
    
    if action not in action_mapping:
        return {
            "error": f"Invalid action '{action}'. Valid actions: {list(action_mapping.keys())}"
        }
    
    item_id = action_mapping[action]
    
    try:
        table = boto3.resource("dynamodb", region_name=region).Table(table_name)
        timestamp = current_timestamp()
        
        table.put_item(Item={
            "stack_id": item_id,
            "timestamp": timestamp,
            "action": action
        })
        
        return {
            "status": "Timestamp stored successfully!",
            "saved_id": item_id,
            "timestamp": timestamp,
            "action": action
        }
    except Exception as e:
        return {
            "error": f"Error storing timestamp: {str(e)}"
        }

def get_stack_deletion_record(item_id, table_name=stack_table_name, region=None):
    """
    Get stack deletion record by ID
    
    Args:
        item_id (str): The ID to search for
        table_name (str, optional): DynamoDB table name
        region (str, optional): AWS region
    
    Returns:
        dict: Response with record data or error message
    """
    print(12345675432)
    if table_name is None:
        table_name = stack_table_name
    if region is None:
        region = DEFAULT_REGION
    
    try:
        table = boto3.resource("dynamodb", region_name=region).Table(table_name)
        response = table.get_item(Key={"stack_id": item_id})
        
        if "Item" in response:
            return {
                "status": 200,
                "data": response["Item"]
            }
        else:
            return {
                "status": 400,
                "error": f"No record found with id: {item_id}"
            }
    except Exception as e:
        return {
            "status": 404,
            "error": f"Error fetching record: {str(e)}"
        }


# # Example usage functions
# def example_usage():
#     """Example of how to use the functions"""
    
#     # Store a timestamp for rag_delete action
#     store_response = store_stack_deletion_timestamp("rag_delete")
#     print("Store Response:", store_response)
    
#     # Get record by ID
#     get_response = get_stack_deletion_record("1")
#     print("Get Response:", get_response)
    
#     # Get record by action name
#     get_by_action_response = get_stack_deletion_record_by_action("rag_delete")
#     print("Get by Action Response:", get_by_action_response)

# if __name__ == "__main__":
#     example_usage()