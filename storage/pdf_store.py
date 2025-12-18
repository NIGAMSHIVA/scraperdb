from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

load_dotenv()

client = MongoClient(os.getenv("MONGO_URI"))
db = client["tender_db"]
collection = db["tender_documents"]

def save_pdf_metadata(data):
    collection.insert_one({
        **data,
        "downloaded_at": datetime.utcnow()
    })
