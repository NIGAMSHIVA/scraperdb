# storage/gridfs_store.py
#  Purpose:
# - Store PDF bytes in MongoDB using GridFS
# - Avoid duplicate uploads (same source + tender_ref_no + pdf_url)
# - Return gridfs_id for reference

from __future__ import annotations

import hashlib
from typing import Optional, Dict, Any

from pymongo.database import Database
from gridfs import GridFS


def _hash_key(source: str, tender_ref_no: str, pdf_url: str) -> str:
    raw = f"{source}::{tender_ref_no}::{pdf_url}".encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


class PdfGridFsStore:
    def __init__(self, db: Database, bucket_name: str = "pdfs"):
        self.db = db
        self.fs = GridFS(db, collection=bucket_name)
        self.meta = db[f"{bucket_name}_meta"]

        #  Unique key so we don’t store same PDF twice
        self.meta.create_index(
            [("key", 1)],
            unique=True,
            name="uniq_pdf_key"
        )

    def put_pdf(
        self,
        *,
        source: str,
        tender_ref_no: str,
        pdf_url: str,
        filename: str,
        content_type: str,
        data: bytes,
        extra_meta: Optional[Dict[str, Any]] = None
    ):
        key = _hash_key(source, tender_ref_no, pdf_url)

        existing = self.meta.find_one({"key": key})
        if existing:
            return existing["gridfs_id"]

        #  Save to GridFS
        gridfs_id = self.fs.put(
            data,
            filename=filename,
            content_type=content_type,
            metadata={
                "source": source,
                "tender_ref_no": tender_ref_no,
                "pdf_url": pdf_url,
                **(extra_meta or {})
            }
        )

        # Save mapping record
        self.meta.insert_one({
            "key": key,
            "gridfs_id": gridfs_id,
            "source": source,
            "tender_ref_no": tender_ref_no,
            "pdf_url": pdf_url,
            "filename": filename,
            "content_type": content_type
        })

        return gridfs_id
