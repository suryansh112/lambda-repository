import boto3
import logging 
import json 

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def format_with_bedrock_claude(schema_data, table_names):
    """
    Use Amazon Bedrock with Claude 3 Haiku to format the schema data
    """
    logger.info(f"********************Inside Bedrock function********************: {table_names}")
    try:
        # Initialize Bedrock client
        bedrock_runtime = boto3.client('bedrock-runtime')

        # Prepare the prompt for Claude
        prompt = f"""
        I have database schema information that I need you to format in a specific way.

        Here's the schema information:
        {json.dumps(schema_data, indent=2)}

        Please format this information for each table and column in the following structure:


       table_name:

         columns:

       column_name:
         type: [data type]
         description: [brief description of what this column represents]
         usage: [how this column is typically used in analysis or applications]
         values: [sample values or value format if applicable]

       [next column...]



        For each column, infer a meaningful description and usage based on the column name and data type.
        If sample values are available, include them in the values field.
        Format the response similar to the example below:



       ipl_summary:

         columns:

       season:
         type: int
         description: The year in which the IPL season took place.
         usage: Numerical variable representing the IPL season year.
         value: 2008 to 2023

       id:
         type: int
         description: Unique identifier for each match.
         usage: Primary key for the matches.



        Only respond with the formatted output, no additional explanations.
        """

        # Call Claude 3 Haiku through Bedrock
        response = bedrock_runtime.invoke_model(
            modelId='anthropic.claude-3-haiku-20240307-v1:0',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4096,
                "temperature": 0,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ]
            })
        )

        # Parse the response
        response_body = json.loads(response['body'].read())
        formatted_schema = response_body['content'][0]['text']
        formatted_schema = f"Based on the following text query, generate a SQL query: My table info: {formatted_schema}\n\nText Query:\n$text_query$\n\nPlease generate a SQL query based on the information provided above.",
        logger.info(f"********************Inside Bedrock function after processing the schema becomes \n********************: {formatted_schema}")

        return {
            'statusCode': 200,
            'body': {
                'formatted_schema': formatted_schema,
                # 'raw_schemas': schema_data['body']['schemas']
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'error': f"Error formatting with Bedrock: {str(e)}",
            'raw_schemas': schema_data['body']['schemas']
        }
