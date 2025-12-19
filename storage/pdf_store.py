from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["tender_db"]
collection = db["tender_documents"]

def save_pdf_metadata(data):
    unique_filter = {
        "source": data["source"],
        "tender_ref_no": data["tender_ref_no"],
    }

    update_data = {
        "$setOnInsert": {
            **data,
            "downloaded_at": datetime.utcnow()
        }
    }

    collection.update_one(
        unique_filter,
        update_data,
        upsert=True
    )
