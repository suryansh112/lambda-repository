import boto3
from utils import MODEL_ARN_MAP
from handlers.chatbot_rag import handle_rag_stage
from handlers.chatbot_text2sql import handle_text2sql_stage
from handlers.ocr import handle_ocr_stage

def handle_stage(stage, body, region, table_name):
    table = boto3.resource("dynamodb", region_name=region).Table(table_name)
    aisuit_models = list(MODEL_ARN_MAP.keys())

    if stage == "start":
        return {
            "question": "Welcome Admin! Please choose an option:",
            "options": ["Chatbot", "OCR"],
            "next_stage": "select_mode"
        }

    elif stage == "select_mode":
        choice = body.get("choice", "").lower()
        if choice in ["chatbot", "ocr"]:
            return {
                "question": "Enter AWS Region (default: ap-south-1):",
                "next_stage": f"{choice}_region"
            }
        return {"error": "Invalid choice."}

    elif stage == "chatbot_region":
        return {"question": "Enter the chatbot name:", "next_stage": "chatbot_name"}

    elif stage == "ocr_region":
        return {"question": "Enter your name:", "next_stage": "ocr_name"}

    elif stage == "chatbot_name":
        return {"question": "Enter your email:", "next_stage": "chatbot_email"}

    elif stage == "chatbot_email":
        return {
            "question": "Select the model (AISuit Layer):",
            "options": aisuit_models,
            "next_stage": "chatbot_model"
        }

    elif stage == "chatbot_model":
        return {
            "question": "Select the language:",
            "next_stage": "chatbot_language",
            "model_arn": MODEL_ARN_MAP.get(body.get("choice", ""), "")
        }

    elif stage == "chatbot_language":
        return {"question": "Enter the temperature value (0.0 to 1.0):", "next_stage": "chatbot_temp"}

    elif stage == "chatbot_temp":
        return {"question": "Select chatbot type:", "options": ["RAG", "Text2SQL"], "next_stage": "chatbot_type"}

    elif stage.startswith("rag_") or (stage == "chatbot_type" and body.get("choice", "").lower() == "rag"):
        return handle_rag_stage(stage, body, region, table)

    elif stage.startswith("text2sql_") or (stage == "chatbot_type" and body.get("choice", "").lower() == "text2sql"):
        return handle_text2sql_stage(stage, body, region, table)

    elif stage.startswith("ocr_"):
        return handle_ocr_stage(stage, body, region, table)

    elif stage == "fetch_record":
        item_id = body.get("id")

        if not item_id:
            return {"error": "Missing 'id' in request."}

        try:
            result = table.get_item(Key={"id": item_id})
            if "Item" in result:
                return {
                    "status": "Record found",
                    "data": result["Item"]
                }
            else:
                return {"error": f"No record found with id: {item_id}"}
        except Exception as e:
            return {"error": f"Error fetching record: {str(e)}"}

    elif stage == "delete_record":
        item_id = body.get("id")

        if not item_id:
            return {"error": "Missing 'id' in request."}

        try:
            result = table.delete_item(
                Key={"id": item_id},
                ReturnValues="ALL_OLD"
            )

            if "Attributes" in result:
                return {
                    "status": "Success",
                    "message": f"Record with id: {item_id} has been deleted",
                    "deleted_data": result["Attributes"]
                }
            else:
                return {"error": f"No record found with id: {item_id}"}
        except Exception as e:
            return {"error": f"Error deleting record: {str(e)}"}