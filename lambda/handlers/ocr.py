from utils import MODEL_ARN_MAP
from datetime import datetime, timedelta

def handle_ocr_stage(stage, body, region, table):
    if stage == "ocr_name":
        return {"question": "Enter your email:", "next_stage": "ocr_email"}

    elif stage == "ocr_email":
        return {
            "question": "Select the model (AISuit Layer):",
            "options": list(MODEL_ARN_MAP.keys()),
            "next_stage": "ocr_model"
        }

    elif stage == "ocr_model":
        return {
            "question": "Enter temperature:",
            "next_stage": "ocr_temp",
            "model_arn": MODEL_ARN_MAP.get(body.get("choice", ""), "")
        }

    elif stage == "ocr_temp":
        return {"question": "Enter max token limit:", "next_stage": "ocr_max_token"}

    elif stage == "ocr_max_token":
        return {
            "question": "Select the language:",
            "options": ["Hindi", "English", "Marathi", "Hinglish", "Kannada", "Tamil", "Telugu"],
            "next_stage": "ocr_language"
        }

    elif stage == "ocr_language":
        return {"question": "Enter the prompt:", "next_stage": "ocr_prompt"}

    elif stage == "ocr_prompt":
        # Get current timestamp in IST format (UTC+5:30)
        timestamp = (datetime.utcnow() + timedelta(hours=5, minutes=30)).isoformat() + '+05:30'
        table.put_item(Item={
            "id": "3",
            "timestamp": timestamp,
            "mode": "OCR",
            "name": body.get("name", ""),
            "email": body.get("email", ""),
            "model": body.get("model", ""),
            "model_arn": MODEL_ARN_MAP.get(body.get("model", ""), ""),
            "temperature": body.get("temperature", ""),
            "max_token": body.get("max_token", ""),
            "language": body.get("language", ""),
            "prompt": body.get("prompt", ""),
            "region": region,
            "status": "NA"
        })
        return {"status": " OCR setup complete!", "saved_id": "3", "timestamp": timestamp}
