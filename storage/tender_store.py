from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

mongo_uri = os.getenv("MONGO_URI") or "mongodb://localhost:27017"
client = MongoClient(mongo_uri)
db = client["tender_db"]
collection = db["raw_tenders"]

def upsert_tender(data):
    """
    One tender = one document
    Stable across re-scrapes
    """

    filter_query = {
        "source": data["source"],
        "tender_ref_no": data["tender_ref_no"]
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

    result = collection.update_one(
        filter_query,
        update_data,
        upsert=True
    )

    if result.upserted_id:
        return result.upserted_id

    doc = collection.find_one(filter_query, {"_id": 1})
    return doc["_id"]
