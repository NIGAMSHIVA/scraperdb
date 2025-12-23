# api/services/mongo.py

import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

_MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
_DB_NAME = "tender_db"

_client = None

def get_db():
    global _client
    if _client is None:
        _client = MongoClient(_MONGO_URI)
    return _client[_DB_NAME]
