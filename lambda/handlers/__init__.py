# import json
# from stages import handle_stage
# from utils import ensure_table_exists, DEFAULT_TABLE

# def lambda_handler(event, context):
#     if isinstance(event.get("body"), str):
#         body = json.loads(event["body"])
#     else:
#         body = event.get("body", event)

#     stage = body.get("stage", "start").lower()
#     region = body.get("region", "").strip() or "ap-south-1"
#     table_name = DEFAULT_TABLE 

#     ensure_table_exists(table_name)
#     response = handle_stage(stage, body, region, table_name)

#     return {
#         "statusCode": 200,
#         "body": response,
#         "headers": {"Content-Type": "application/json"}
#     }
