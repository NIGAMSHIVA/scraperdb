# storage/tender_docs_store.py
#  Purpose:
# - Store “tender -> pdfs metadata” into tender_db.tender_documents
# - Track status: DISCOVERED / DOWNLOADED / FAILED
# - Upsert per tender_ref_no

from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List
from pymongo.collection import Collection


class TenderDocumentsStore:
    def __init__(self, collection: Collection):
        self.collection = collection

        self.collection.create_index(
            [("source", 1), ("tender_ref_no", 1)],
            unique=True,
            name="uniq_tender_doc"
        )

    def upsert_discovered(
        self,
        *,
        source: str,
        tender_ref_no: str,
        detail_url: str,
        pdf_links: List[Dict[str, Any]]
    ):
        now = datetime.utcnow()
        self.collection.update_one(
            {"source": source, "tender_ref_no": tender_ref_no},
            {
                "$setOnInsert": {"created_at": now},
                "$set": {
                    "detail_url": detail_url,
                    "status": "DISCOVERED",
                    "updated_at": now,
                },
                "$addToSet": {
                    #  keep unique pdf_urls inside pdfs array
                    "pdfs": {"$each": pdf_links}
                }
            },
            upsert=True
        )

    def mark_downloaded(
        self,
        *,
        source: str,
        tender_ref_no: str,
        pdf_entry: Dict[str, Any]
    ):
        now = datetime.utcnow()
        self.collection.update_one(
            {"source": source, "tender_ref_no": tender_ref_no},
            {
                "$set": {"updated_at": now, "status": "DOWNLOADED"},
                "$addToSet": {"pdfs": pdf_entry}
            },
            upsert=True
        )

    def mark_failed(
        self,
        *,
        source: str,
        tender_ref_no: str,
        error: str
    ):
        now = datetime.utcnow()
        self.collection.update_one(
            {"source": source, "tender_ref_no": tender_ref_no},
            {
                "$set": {
                    "status": "FAILED",
                    "error": error,
                    "updated_at": now
                }
            },
            upsert=True
        )
