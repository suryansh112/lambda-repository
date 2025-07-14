from utils import MODEL_ARN_MAP
from datetime import datetime, timedelta
import boto3

def handle_rag_stage(stage, body, region, table):
    if stage == "chatbot_type":
        return {
            "question": "Do you want to create a new KB?",
            "options": ["Yes", "No"],
            "next_stage": "rag_kb"
        }

    elif stage == "rag_kb":
        if body.get("choice", "").lower() == "yes":
            return {
                "question": "Choose a VectorDB:",
                "options": ["PostgreSQL", "OpenSearch Serverless"],
                "next_stage": "rag_vector_db"
            }
        else:
            return {
                "question": "Enter existing knowledge base name:",
                "next_stage": "rag_existing_kb"
            }

    elif stage == "rag_existing_kb":
        return {
            "question": "Choose a VectorDB:",
            "options": ["PostgreSQL", "OpenSearch Serverless"],
            "next_stage": "rag_vector_db"
        }

    elif stage == "rag_vector_db":
        return {
            "question": "Enter the prompt for the chatbot:",
            "next_stage": "rag_prompt"
        }

    elif stage == "rag_prompt":
        
        # Get current timestamp in IST format (UTC+5:30)
        timestamp = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat() + '+05:30'
        table.put_item(Item={
            "id": "1",
            "timestamp": timestamp,
            "mode": "Chatbot",
            "chatbot_type": "RAG",
            "chatbot_name": body.get("chatbot_name", ""),
            "email": body.get("email", ""),
            "model": body.get("model", ""),
            "model_arn": MODEL_ARN_MAP.get(body.get("model", ""), ""),
            "language": body.get("language", ""),
            "temperature": body.get("temperature", ""),
            "vectordb": body.get("vectordb", ""),
            "prompt": body.get("prompt", ""),
            "region": region,
            "new_kb": body.get("choice", ""),
            "existing_kb": body.get("existing_kb", ""),
            "status": "NA"
        })
        return {"status": " RAG Chatbot setup complete!", "saved_id": "1", "timestamp": timestamp}
