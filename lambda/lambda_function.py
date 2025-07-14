import json
import sys
import os
from datetime import date, datetime
import boto3
import logging
import decimal
from stages import handle_stage
from utils import ensure_table_exists, DEFAULT_TABLE
from database import *
from bedrock_utils import *
from admin_page import *
import time
from retrieve_parameter import get_parameter_value
from ses_trigger import *


# Get the logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Updated JSON encoder to handle dates, Decimal, and bytes objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, bytes):  # Add handling for bytes
            return obj.decode('utf-8', errors='replace')  # Convert bytes to string
        return super().default(obj)


def lambda_handler(event, context):
    print("Event", event)

    if "action" in event and event.get('action') == 'get_id_details':
        a = event.get('stack')
        res1 = get_id_details(a)
        
        res1["status"] = "Deletion initiated...."

        return update_status(res1)

    elif "action" in event and event.get('action') == 'send_email':
        email = event.get('email')
        group = event.get('group')
        Application_Link = event.get('Application_Link')
        return send_email(email, group, Application_Link)

    elif "action" in event and event.get('action') == 'add_ses':
        domain = event.get('domain')
        region = event.get('region')    
        Client_SES = event.get('Client_SES')
        return add_ses_details(domain, region, Client_SES)

    elif "action" in event and event.get('action') == 'send_email':
        email = event.get('email')
        group = event.get('group')
        Application_Link = event.get('Application_Link')
        return send_email(email, group, Application_Link)

    elif "action" in event and event.get('action') == 'get_id_stack_deletion_timestamp':
        print(1222,event)
        item_id = event.get('stack_id')
        return get_stack_deletion_record(item_id)

    # Check if this is a CloudFormation action request
    elif event.get('action') in ['ocr_create', 'rag_create', 'text2SQL_create',
                              'ocr_delete', 'rag_delete', 'text2SQL_delete', 'admin_delete', 'admin_create', 'rag_CDN_url', 'ocr_CDN_url', 'text2SQL_CDN_url', 'admin_CDN_url']:
        return handle_cloudformation_action(event)

    # Check if this is a database schema request
    elif "method" in event and event["method"] == "test_db":
        return test_db(event)

    elif event.get('db_type') and (event.get('host') or event.get('method')):
        return handle_db_schema_request(event, context)

    # Original lambda handler code
    # if isinstance(event.get("body"), str):
    #     body = json.loads(event["body"])
    else:
        body = event.get("body", event)

        stage = body.get("stage", "start").lower()
        region = body.get("region", "").strip() or "ap-south-1"
        table_name = DEFAULT_TABLE

        ensure_table_exists(table_name)
        response = handle_stage(stage, body, region, table_name)

        return {
            "statusCode": 200,
            "body": response,
            "headers": {"Content-Type": "application/json"}
        }



def empty_s3_bucket(bucket_name):
    """Empty an S3 bucket to allow deletion"""
    try:
        logger.info(f"Emptying S3 bucket: {bucket_name}")
        s3 = boto3.resource('s3')
        s3_client = boto3.client('s3')
        bucket = s3.Bucket(bucket_name)

        # Check if bucket exists
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception as e:
            logger.warning(f"Bucket {bucket_name} does not exist or cannot be accessed: {str(e)}")
            return False

        # Check if versioning is enabled
        versioning = s3_client.get_bucket_versioning(Bucket=bucket_name)
        versioning_status = versioning.get('Status', 'Disabled')

        if versioning_status == 'Enabled':
            logger.info(f"Bucket {bucket_name} has versioning enabled. Deleting all versions.")

            # Continue deleting versions until the bucket is empty
            version_paginator = s3_client.get_paginator('list_object_versions')

            for page in version_paginator.paginate(Bucket=bucket_name):
                # Delete versions
                versions = page.get('Versions', [])
                if versions:
                    objects_to_delete = [{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in versions]
                    if objects_to_delete:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )

                # Delete delete markers
                delete_markers = page.get('DeleteMarkers', [])
                if delete_markers:
                    markers_to_delete = [{'Key': obj['Key'], 'VersionId': obj['VersionId']} for obj in delete_markers]
                    if markers_to_delete:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': markers_to_delete}
                        )
        else:
            # For non-versioned buckets, use pagination to handle large buckets
            object_paginator = s3_client.get_paginator('list_objects_v2')

            for page in object_paginator.paginate(Bucket=bucket_name):
                objects = page.get('Contents', [])
                if objects:
                    objects_to_delete = [{'Key': obj['Key']} for obj in objects]
                    if objects_to_delete:
                        s3_client.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )

        logger.info(f"Successfully emptied S3 bucket: {bucket_name}")
        return True
    except Exception as e:
        logger.error(f"Error emptying S3 bucket {bucket_name}: {str(e)}")
        return False

def get_stack_resources(stack_name):
    """Get all resources in a CloudFormation stack"""
    cf_client = boto3.client('cloudformation')
    try:
        response = cf_client.list_stack_resources(StackName=stack_name)
        return response.get('StackResourceSummaries', [])
    except Exception as e:
        logger.error(f"Error getting stack resources: {str(e)}")
        return []

def get_cloudfront_distribution_id(stack_name):
    """Get CloudFront distribution ID from stack resources"""
    try:
        cf = boto3.client('cloudformation')
        response = cf.list_stack_resources(StackName=stack_name)

        # Look for CloudFront distribution in the resources
        for resource in response.get('StackResourceSummaries', []):
            if resource['ResourceType'] == 'AWS::CloudFront::Distribution':
                return resource.get('PhysicalResourceId')

        logger.warning(f"No CloudFront distribution found in stack {stack_name}")
        return None
    except Exception as e:
        logger.error(f"Error getting CloudFront distribution ID: {str(e)}")
        return None



def get_cloudfront_domain_name(distribution_id):
    """Get CloudFront distribution domain name from distribution ID"""
    try:
        cloudfront = boto3.client('cloudfront')
        response = cloudfront.get_distribution(Id=distribution_id)
        domain_name = response['Distribution']['DomainName']
        logger.info(f"Retrieved domain name for distribution {distribution_id}: {domain_name}")
        return domain_name
    except Exception as e:
        logger.error(f"Error getting CloudFront domain name for distribution {distribution_id}: {str(e)}")
        return None






def handle_cloudformation_action(event):
    """
    Handle CloudFormation stack deployment or deletion
    """
    try:
        # Extract action from the event
        action = event.get('action')
        admin_email = event.get('admin_email')
        apiendpointadmin = event.get('apiendpointadmin')
        bucket_name = event.get('bucket_name')

        if not action:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': 'Missing required parameter: action'
                })
            }

        # Initialize variables
        s3_url = None
        stack_name = None
        stack_type = None
        capabilities = ['CAPABILITY_IAM', 'CAPABILITY_NAMED_IAM', 'CAPABILITY_AUTO_EXPAND']
        operation = None
        parameters = []

        # Handle CDN URL actions
        if action in ['rag_CDN_url', 'ocr_CDN_url', 'text2SQL_CDN_url', 'admin_CDN_url']:
            # Determine which stack name to use based on the action
            if action == 'rag_CDN_url':
                stack_name = get_parameter_value('rag_stack_name')
            elif action == 'ocr_CDN_url':
                stack_name = get_parameter_value('ocr_stack_name')
            elif action == 'text2SQL_CDN_url':
                stack_name = get_parameter_value('text2SQL_stack_name')
            elif action == 'admin_CDN_url':
                stack_name = get_parameter_value('Admin_Stack_name')
            print("-------------stack name ----------------")
            print(stack_name)

            logger.info(f"Using stack name {stack_name} for action {action}")

            if not stack_name:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'message': f'Missing environment variable for stack name related to action {action}'
                    })
                }

            cf = boto3.client('cloudformation')
            response = cf.list_stack_resources(StackName=stack_name)

            # Find CloudFront distribution in the resources
            cloudfront_id = None
            for resource in response.get('StackResourceSummaries', []):
                if resource['ResourceType'] == 'AWS::CloudFront::Distribution':
                    cloudfront_id = resource.get('PhysicalResourceId')
                    break

            if cloudfront_id:
                # Get the CloudFront domain name
                domain_name = get_cloudfront_domain_name(cloudfront_id)

                if domain_name:
                    return {
                        'statusCode': 200,
                        'body': {
                            'message': f'CloudFront distribution information retrieved for {action}',
                            'cloudfront_distribution_id': cloudfront_id,
                            'cloudfront_domain_name': domain_name
                        }
                    }
                else:
                    return {
                        'statusCode': 404,
                        'body': json.dumps({
                            'message': f'CloudFront distribution found but could not retrieve domain name',
                            'cloudfront_distribution_id': cloudfront_id
                        })
                    }
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'message': f'CloudFront distribution not found in stack {stack_name}'
                    })
                }

        # Determine stack type and operation
        if action == 'ocr_create':
            stack_type = 'ocr'
            operation = 'create'
            s3_url = get_parameter_value('ocr_s3_uri')
            stack_name = get_parameter_value('ocr_stack_name')
        elif action == 'rag_create':
            stack_type = 'rag'
            operation = 'create'
            s3_url = get_parameter_value('rag_s3_uri')
            stack_name = get_parameter_value('rag_stack_name')
        elif action == 'text2SQL_create':
            stack_type = 'text2SQL'
            operation = 'create'
            s3_url = get_parameter_value('text2SQL_s3_uri')
            stack_name = get_parameter_value('text2SQL_stack_name')
        elif action == 'admin_create':
            stack_type = 'teststack'
            operation = 'create'
            # s3_url = os.environ.get('test_url')
            # stack_name = os.environ.get('test_Stack')
        elif action == 'ocr_delete':
            stack_type = 'ocr'
            operation = 'delete'
            s3_url = get_parameter_value('ocr_s3_uri')
            stack_name = get_parameter_value('ocr_stack_name')
        elif action == 'rag_delete':
            stack_type = 'rag'
            operation = 'delete'
            s3_url = get_parameter_value('rag_s3_uri')
            stack_name = get_parameter_value('rag_stack_name')
        elif action == 'text2SQL_delete':
            stack_type = 'text2SQL'
            operation = 'delete'
            s3_url = get_parameter_value('text2SQL_s3_uri')
            stack_name = get_parameter_value('text2SQL_stack_name')
        elif action == 'admin_delete':
            stack_type = 'admin_stack'
            operation = 'delete'
            stack_name = get_parameter_value('Admin_Stack_name')
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Invalid action: {action}. Must be one of: ocr_create, rag_create, text2SQL_create, admin_create, ocr_delete, rag_delete, text2SQL_delete, admin_delete, rag_CDN_url, ocr_CDN_url, text2SQL_CDN_url, admin_CDN_url'
                })
            }

        logger.info(f"Processing {operation} for {stack_type} stack: {stack_name}, template: {s3_url}")

        # Validate required parameters
        if not s3_url and operation != 'delete':
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Missing environment variables for {stack_type}. Please check that {stack_type}_s3_uri is set correctly.'
                })
            }

        if not stack_name:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'message': f'Missing environment variables for {stack_type}. Please check that {stack_type}_stack_name is set correctly.'
                })
            }

        # Add parameters for create operations
        if operation == 'create':
            # Add admin_email parameter if provided
            if admin_email:
                logger.info(f"Adding admin email parameter: {admin_email}")
                parameters.append({
                    'ParameterKey': 'AdminEmail',
                    'ParameterValue': admin_email
                })

            if apiendpointadmin:
                logger.info(f"Adding API endpoint admin parameter: {apiendpointadmin}")
                parameters.append({
                    'ParameterKey': 'ApiEndpointAdmin',
                    'ParameterValue': apiendpointadmin
                })

            # Add BucketName parameter if provided or generate a unique one
            if bucket_name:
                logger.info(f"Adding bucket name parameter: {bucket_name}")
            else:
                # Generate a unique bucket name if not provided
                import uuid
                unique_id = str(uuid.uuid4())[:8]
                bucket_name = f"{stack_type}-bucket-{unique_id}"
                logger.info(f"Generated unique bucket name: {bucket_name}")

            # parameters.append({
            #     'ParameterKey': 'BucketName',
            #     'ParameterValue': bucket_name
            # })

        # Create CloudFormation client
        cf_client = boto3.client('cloudformation')

        # Handle delete operation
        if operation == 'delete':
            try:
                # Check if stack exists before attempting to delete
                a = cf_client.describe_stacks(StackName=stack_name)

                # Get all resources in the stack
                resources = get_stack_resources(stack_name)
                logger.info(f"Resources in stack {stack_name}: {resources}")
    
                # Main logic to update the status in Admin DynamoDB
                res1 = get_id_details(action)
                if "error" not in res1:
                    res2 = copy.deepcopy(res1)
                    res2["status"] = "Stack Deletion initiated...."
                    update_status(res2)

                # Find and empty all S3 buckets
                for resource in resources:
                    if resource['ResourceType'] == 'AWS::S3::Bucket':
                        bucket_name = resource.get('PhysicalResourceId')
                        if bucket_name:
                            logger.info(f"Emptying S3 bucket before deletion: {bucket_name}")
                            empty_s3_bucket(bucket_name)

                # Now delete the stack
                delete_response = cf_client.delete_stack(StackName=stack_name)

                # store the stack deletion timestamp
                store_stack_deletion_timestamp(action)
        
                # Poll for deletion
                def poll_stack_status(stack_id, wait_interval=300, timeout=800):
                    """
                    Polls the CloudFormation stack status every `wait_interval` seconds 
                    until a terminal state is reached or timeout occurs.
                    
                    Parameters:
                    - stack_id: ID or name of the CloudFormation stack
                    - wait_interval: Time (in seconds) to wait between polls. Default is 300 seconds (5 minutes).
                    - timeout: Total time (in seconds) to wait before giving up. Default is 800 seconds.
                    """
                    start_time = time.time()
                    terminal_states = [
                        "CREATE_COMPLETE", "CREATE_FAILED", 
                        "ROLLBACK_COMPLETE", "ROLLBACK_FAILED",
                        "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
                        "UPDATE_FAILED", "DELETE_COMPLETE", "DELETE_FAILED"
                    ]
                    response = cf_client.describe_stacks(StackName=stack_id)
                    status = response['Stacks'][0]['StackStatus']
                    print(status)
                    # Update status in DynamoDB
                    if "error" not in res1:
                        res2 = copy.deepcopy(res1)
                        res2["status"] = status
                        update_status(res2)

                    time.sleep(wait_interval)

                    while True:
                        response = cf_client.describe_stacks(StackName=stack_id)
                        status = response['Stacks'][0]['StackStatus']
                        print(status)

                        if status in terminal_states:
                            return status

                        elapsed_time = time.time() - start_time
                        if elapsed_time > timeout:
                            return "TIMEOUT" 

                # Wait for deletion to complete
                final_status = poll_stack_status(stack_name)

                # Final status update
                if "error" not in res1:
                    res2 = copy.deepcopy(res1)
                    res2["status"] = final_status
                    update_status(res2)


                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'{stack_type} stack deletion initiated for {stack_name} with status: {final_status}',
                        'requestId': delete_response.get('ResponseMetadata', {}).get('RequestId')
                    })
                }
            except cf_client.exceptions.ClientError as e:
                if 'does not exist' in str(e):
                    print("ABV")
                    # Main logic to update the status in Admin DynamoDB
                    res1 = get_id_details(action)
                    if "error" not in res1:
                        res2 = copy.deepcopy(res1)
                        res2["status"] = "DELETE_COMPLETE"
                        update_status(res2)
                    return {
                        'statusCode': 404,
                        'body': json.dumps({
                            'message': f'{stack_type} stack {stack_name} does not exist'
                        })
                    }
                else:
                    raise

        # Check if stack exists to determine if we should create or update
        stack_exists = False
        try:
            cf_client.describe_stacks(StackName=stack_name)
            stack_exists = True
            operation = 'update'  # If stack exists, change to update
        except cf_client.exceptions.ClientError:
            operation = 'create'  # If stack doesn't exist, ensure operation is create

        # For create or update operations, use the template URL directly
        if operation == 'create':
            try:
                create_params = {
                    'StackName': stack_name,
                    'TemplateURL': s3_url,
                    'Capabilities': capabilities
                }

                # Add parameters if they exist
                if parameters:
                    create_params['Parameters'] = parameters

                
                create_response = cf_client.create_stack(**create_params)

                stack_id = create_response.get('StackId')

                # Main logic to update the status in Admin DynamoDB
                res1 = get_id_details(action)
                print("Res1", res1)
                if "error" not in res1:
                    res2 = copy.deepcopy(res1)
                    res2["status"] = "Stack Creation initiated...."
                    update_status(res2)

                def poll_stack_status(stack_id, wait_interval=320, timeout=800):
                    """
                    Polls the CloudFormation stack status every `wait_interval` seconds 
                    until a terminal state is reached or timeout occurs.
                    
                    Parameters:
                    - stack_id: ID or name of the CloudFormation stack
                    - wait_interval: Time (in seconds) to wait between polls. Default is 300 seconds (5 minutes).
                    - timeout: Total time (in seconds) to wait before giving up. Default is 800 seconds.
                    """
                    start_time = time.time()
                    terminal_states = [
                        "CREATE_COMPLETE", "CREATE_FAILED", 
                        "ROLLBACK_COMPLETE", "ROLLBACK_FAILED",
                        "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
                        "UPDATE_FAILED", "DELETE_COMPLETE", "DELETE_FAILED"
                    ]

                    response = cf_client.describe_stacks(StackName=stack_id)
                    status = response['Stacks'][0]['StackStatus']
                    print(status)
                    # Update status in DynamoDB
                    if "error" not in res1:
                        res2 = copy.deepcopy(res1)
                        res2["status"] = status
                        print("Res2", res2)
                        update_status(res2)

                    time.sleep(wait_interval)

                    while True:
                        response = cf_client.describe_stacks(StackName=stack_id)
                        status = response['Stacks'][0]['StackStatus']
                        print(status)

                        if status in terminal_states:
                            return status

                        elapsed_time = time.time() - start_time
                        if elapsed_time > timeout:
                            return "TIMEOUT" 


                # Wait for stack to complete or fail
                final_status = poll_stack_status(stack_id)

                # Final status update
                if "error" not in res1:
                    res2 = copy.deepcopy(res1)
                    res2["status"] = final_status
                    print("Res2 502", res2)
                    update_status(res2)

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'{stack_type} stack creation initiated for {stack_name} with status: {final_status}',
                        'stackId': create_response.get('StackId'),
                        'requestId': create_response.get('ResponseMetadata', {}).get('RequestId'),
                        'bucketName': bucket_name  # Return the bucket name used
                    })
                }
            except Exception as e:
                logger.error(f"Error creating stack: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': f'Error creating {stack_type} stack {stack_name}',
                        'error': str(e)
                    })
                }

        elif operation == 'update':
            try:
                update_params = {
                    'StackName': stack_name,
                    'TemplateURL': s3_url,
                    'Capabilities': capabilities
                }

                # Add parameters if they exist
                if parameters:
                    update_params['Parameters'] = parameters

                update_response = cf_client.update_stack(**update_params)

                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': f'{stack_type} stack update initiated for {stack_name}',
                        'stackId': update_response.get('StackId'),
                        'requestId': update_response.get('ResponseMetadata', {}).get('RequestId'),
                        'bucketName': bucket_name  # Return the bucket name used
                    })
                }
            except cf_client.exceptions.ClientError as e:
                error_message = str(e)

                # Handle the "No updates are to be performed" error
                if "No updates are to be performed" in error_message:
                    return {
                        'statusCode': 200,
                        'body': json.dumps({
                            'message': f'No updates needed for {stack_type} stack {stack_name}',
                            'status': 'NO_UPDATES_NEEDED'
                        })
                    }
                else:
                    logger.error(f"Error updating stack: {error_message}")
                    return {
                        'statusCode': 500,
                        'body': json.dumps({
                            'message': f'Error updating {stack_type} stack {stack_name}',
                            'error': error_message
                        })
                    }
            except Exception as e:
                logger.error(f"Unexpected error updating stack: {str(e)}")
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': f'Unexpected error updating {stack_type} stack {stack_name}',
                        'error': str(e)
                    })
                }

    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e)
            })
        }




def handle_db_schema_request(event, context):
    db_type = event.get('db_type')
    raw_host = event.get('host')
    username = event.get('username')
    password = event.get('password')
    dbname = event.get('dbname')
    event_port = event.get('port')  # Get port from event if available
    method = event.get('method')
    format_with_bedrock = event.get('format_with_bedrock', True)
    print(method)

    # Extract host and port from raw_host if port is included
    host = raw_host
    endpoint_port = None

    # Check if host contains port
    if ':' in raw_host:
        parts = raw_host.split(':')
        host = parts[0]  # Always take the first part as the host

        try:
            endpoint_port = int(parts[1])
        except ValueError:
            logger.warning(f"Invalid port format in host: {raw_host}, using default port")
            endpoint_port = None  # Will use default port later

    # Choose port based on priority: event > endpoint > default
    if event_port:
        try:
            port = int(event_port)
        except ValueError:
            logger.warning(f"Invalid port format in event: {event_port}, using default port")
            port = None  # Will use default port later
    elif endpoint_port:
        port = endpoint_port
    else:
        port = None  # Will use default port later

    # Set default ports
    default_port = 3306 if db_type == 'mysql' else 5432

    # If port is still None, use the default
    if port is None:
        port = default_port

    logger.info(f"Attempting to connect to {db_type} database at {host}:{port}")

    if db_type == 'mysql':
        import mysql.connector

        # First try with the specified port
        try:
            conn = mysql.connector.connect(
                host=host,
                user=username,
                password=password,
                port=port,
                database=dbname
            )
            cursor = conn.cursor(dictionary=True)  # Use dictionary cursor to get column names

            # Connection successful, proceed with the request
            if method == "test_db":
                return test_db(event)
            elif method == 'get_tables':
                return get_tables_mysql(cursor)
            elif method == 'select_table':
                table_names = event.get('table_names', [])
                if not table_names:
                    return {'statusCode': 400, 'error': 'Please provide at least one table name'}
                # Limit to maximum 2 tables
                table_names = table_names[:2]
                # Get schema information
                result = describe_tables_mysql(cursor, table_names)

                # Convert to JSON-serializable format
                serializable_result = json.loads(json.dumps(result, cls=DateTimeEncoder))

                # Format with Bedrock if requested
                if format_with_bedrock:
                    return format_with_bedrock_claude(serializable_result, table_names)

                return serializable_result
            else:
                return {'statusCode': 400, 'error': 'Invalid method specified'}

        except Exception as e:
            # If the connection fails and we're not already using the default port, try with default port
            if port != default_port:
                logger.warning(f"Connection failed with port {port}. Trying default port {default_port}. Error: {str(e)}")
                try:
                    conn = mysql.connector.connect(
                        host=host,
                        user=username,
                        password=password,
                        port=default_port,
                        database=dbname
                    )
                    cursor = conn.cursor(dictionary=True)

                    # Connection with default port successful, proceed with the request
                    if method == "test_db":
                        return test_db(event)
                    elif method == 'get_tables':
                        return get_tables_mysql(cursor)
                    elif method == 'select_table':
                        table_names = event.get('table_names', [])
                        if not table_names:
                            return {'statusCode': 400, 'error': 'Please provide at least one table name'}
                        # Limit to maximum 2 tables
                        table_names = table_names[:2]
                        # Get schema information
                        result = describe_tables_mysql(cursor, table_names)

                        # Convert to JSON-serializable format
                        serializable_result = json.loads(json.dumps(result, cls=DateTimeEncoder))

                        # Format with Bedrock if requested
                        if format_with_bedrock:
                            return format_with_bedrock_claude(serializable_result, table_names)

                        return serializable_result
                    else:
                        return {'statusCode': 400, 'error': 'Invalid method specified'}

                except Exception as e2:
                    # Both attempts failed
                    return {'statusCode': 500, 'error': f"Failed to connect with both specified port ({port}) and default port ({default_port}). Error: {str(e2)}"}
            else:
                # We were already using the default port and it failed
                return {'statusCode': 500, 'error': str(e)}
        finally:
            if 'cursor' in locals() and cursor: cursor.close()
            if 'conn' in locals() and conn: conn.close()

    elif db_type == 'postgresql':
        import psycopg2
        import psycopg2.extras

        # First try with the specified port
        try:
            conn = psycopg2.connect(
                host=host,
                user=username,
                password=password,
                port=port,
                dbname=dbname
            )

            # Connection successful, proceed with the request
            if method == 'test_db':
                return test_db(event)
            elif method == 'get_tables':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                return get_tables_postgresql(cursor, username)
            elif method == 'select_table':
                cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                table_names = event.get('table_names', [])
                if not table_names:
                    return {'statusCode': 400, 'error': 'Please provide at least one table name'}
                # Limit to maximum 2 tables
                table_names = table_names[:2]
                # Get schema information
                result = describe_tables_postgresql(cursor, table_names)

                # Convert to JSON-serializable format
                serializable_result = json.loads(json.dumps(result, cls=DateTimeEncoder))

                # Format with Bedrock if requested
                if format_with_bedrock:
                    return format_with_bedrock_claude(serializable_result, table_names)

                return serializable_result
            else:
                return {'statusCode': 400, 'error': 'Invalid method specified'}

        except Exception as e:
            # If the connection fails and we're not already using the default port, try with default port
            if port != default_port:
                logger.warning(f"Connection failed with port {port}. Trying default port {default_port}. Error: {str(e)}")
                try:
                    conn = psycopg2.connect(
                        host=host,
                        user=username,
                        password=password,
                        port=default_port,
                        dbname=dbname
                    )

                    # Connection with default port successful, proceed with the request
                    if method == 'test_db':
                        return test_db(event)
                    elif method == 'get_tables':
                        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                        return get_tables_postgresql(cursor, username)
                    elif method == 'select_table':
                        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                        table_names = event.get('table_names', [])
                        if not table_names:
                            return {'statusCode': 400, 'error': 'Please provide at least one table name'}
                        # Limit to maximum 2 tables
                        table_names = table_names[:2]
                        # Get schema information
                        result = describe_tables_postgresql(cursor, table_names)

                        # Convert to JSON-serializable format
                        serializable_result = json.loads(json.dumps(result, cls=DateTimeEncoder))

                        # Format with Bedrock if requested
                        if format_with_bedrock:
                            return format_with_bedrock_claude(serializable_result, table_names)

                        return serializable_result
                    else:
                        return {'statusCode': 400, 'error': 'Invalid method specified'}

                except Exception as e2:
                    # Both attempts failed
                    return {'statusCode': 500, 'error': f"Failed to connect with both specified port ({port}) and default port ({default_port}). Error: {str(e2)}"}
            else:
                # We were already using the default port and it failed
                return {'statusCode': 500, 'error': str(e)}
        finally:
            if 'cursor' in locals() and cursor: cursor.close()
            if 'conn' in locals() and conn: conn.close()
    else:
        return {'statusCode': 400, 'error': 'Unsupported database type'}





