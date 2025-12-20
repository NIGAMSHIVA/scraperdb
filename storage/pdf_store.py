from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI") or "mongodb://localhost:27017"
client = MongoClient(mongo_uri)
db = client["tender_db"]
collection = db["tender_documents"]

def upsert_pdf_metadata(data):
    """
    One PDF per tender
    Safe for re-runs
    """

    filter_query = {
        "$or": [
            {
                "tender_id": data["tender_id"],
                "document_name": data["document_name"]
            },
            {
                "source": data["source"],
                "tender_ref_no": data["tender_ref_no"]
            }
        ]
    }

    update_data = {
        "$set": {
            **data,
            "updated_at": datetime.utcnow()
        },
        "$setOnInsert": {
            "created_at": datetime.utcnow()
        }
    }

    collection.update_one(
        filter_query,
        update_data,
        upsert=True
    )
