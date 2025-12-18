# main.py
from pymongo import MongoClient
import os
from dotenv import load_dotenv

from scrapers.cppp.cppp_scraper import fetch_cppp_tenders
from scrapers.cppp.cppp_detail_fetcher import fetch_cppp_pdfs_and_store

load_dotenv()

def main():
    #  Step 1: Scrape list data
    fetch_cppp_tenders()

    #  Step 2: Fetch PDFs from detail pages + store locally + GridFS
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(mongo_uri)
    db = client["tender_db"]

    # limit can be increased later
    fetch_cppp_pdfs_and_store(db, limit=30)

if __name__ == "__main__":
    main()
