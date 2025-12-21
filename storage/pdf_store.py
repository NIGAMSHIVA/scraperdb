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

    insert_docling_status = data.get("docling_status", "pending")
    update_payload = {**data}
    update_payload.pop("docling_status", None)

    update_data = {
        "$set": {
            **update_payload,
            "updated_at": datetime.utcnow()
        },
        "$setOnInsert": {
            "created_at": datetime.utcnow(),
            "docling_status": insert_docling_status
        }
    }

    collection.update_one(
        filter_query,
        update_data,
        upsert=True
    )
