from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["tender_db"]
collection = db["raw_tenders"]

def save_raw_tender(data):
    unique_key = {
        "source": data["source"],
        "tender_ref_no": data["tender_ref_no"]
    }

    if collection.find_one(unique_key):
        return

    collection.insert_one(data)
