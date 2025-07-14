from utils import MODEL_ARN_MAP
from datetime import datetime, timedelta

def handle_text2sql_stage(stage, body, region, table):
    if stage == "text2sql_db":
        return {
            "question": "Enter JDBC endpoint:",
            "next_stage": "jdbc_endpoint",
            "db_type": body.get("choice", "")
        }

    elif stage == "jdbc_endpoint":
        return {
            "question": "Enter username:",
            "next_stage": "jdbc_username",
            "db_type": body.get("db_type", "")
        }

    elif stage == "jdbc_username":
        return {
            "question": "Enter password:",
            "next_stage": "jdbc_password",
            "db_type": body.get("db_type", "")
        }

    elif stage == "jdbc_password":
        return {
            "question": "Enter port number:",
            "next_stage": "jdbc_port",
            "db_type": body.get("db_type", "")
        }

    elif stage == "jdbc_port":
        return {
            "question": "Enter the prompt:",
            "next_stage": "text2sql_prompt",
            "db_type": body.get("db_type", "")
        }

    elif stage == "text2sql_prompt":
        # Get current timestamp in IST format (UTC+5:30)
        timestamp = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat() + '+05:30'
        table.put_item(Item={
            "id": "2",
            "timestamp": timestamp,
            "mode": "Chatbot",
            "chatbot_type": "Text2SQL",
            "chatbot_name": body.get("chatbot_name", ""),
            "email": body.get("email", ""),
            "model": body.get("model", ""),
            "model_arn": MODEL_ARN_MAP.get(body.get("model", ""), ""),
            "language": body.get("language", ""),
            "temperature": body.get("temperature", ""),
            "db_type": body.get("db_type", ""),
            "db_name":body.get("db_name",""),
            "jdbc_endpoint": body.get("jdbc_endpoint", ""),
            "jdbc_username": body.get("jdbc_username", ""),
            "jdbc_password": body.get("jdbc_password", ""),
            "jdbc_port": body.get("jdbc_port", ""),
            "prompt": body.get("prompt", ""),
            "region": region,
            "status": "NA"
        })
        return {"status": " Text2SQL setup complete!", "saved_id": "2", "timestamp": timestamp}
