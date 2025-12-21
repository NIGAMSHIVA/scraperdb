# embeddings/index_tenders.py

from __future__ import annotations

import logging
from typing import List
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone
import os

try:
    from embeddings.chunker import chunk_text
    from embeddings.tender_embedder import TenderEmbedder
    from embeddings.vector_store import get_chroma_collection
except ModuleNotFoundError:  # Allow running as a script from this folder.
    from chunker import chunk_text
    from tender_embedder import TenderEmbedder
    from vector_store import get_chroma_collection

load_dotenv()

# ---------------- CONFIG ---------------- #

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "tender_db"

BATCH_SIZE = 64  # safe for CPU
CHUNK_SIZE = 500

# ---------------- INIT ---------------- #

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

docling_outputs = db["docling_outputs"]

collection = get_chroma_collection()
embedder = TenderEmbedder()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


# ---------------- HELPERS ---------------- #

def _combine_text_and_tables(doc: dict) -> str:
    """
    Combine main text + table text for embedding.
    """
    text = doc.get("text", "") or ""
    tables = doc.get("tables", [])

    table_texts: List[str] = []
    for table in tables:
        if isinstance(table, dict):
            table_texts.extend(str(v) for v in table.values())
        else:
            table_texts.append(str(table))

    return "\n".join([text] + table_texts).strip()


def _batch_items(items: List[str], batch_size: int) -> List[List[str]]:
    if batch_size <= 0:
        return [items]
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


# ---------------- MAIN LOGIC ---------------- #

def index_pending_tenders(limit: int = 10) -> None:
    """
    Embed and index tenders that are not yet indexed.
    """

    if CHUNK_SIZE <= 0:
        raise ValueError("CHUNK_SIZE must be greater than 0")

    pending = docling_outputs.find(
        {"indexed": {"$ne": True}},
        limit=limit
    )

    for doc in pending:
        document_id = str(doc["_id"])
        tender_id = doc.get("tender_id")
        tender_id_str = str(tender_id) if tender_id is not None else None
        source = doc.get("source")

        combined_text = _combine_text_and_tables(doc)

        if not combined_text:
            logger.warning("Skipping empty doc: %s", document_id)
            continue

        chunks = chunk_text(combined_text, max_chars=CHUNK_SIZE)

        if not chunks:
            logger.warning("No chunks created for: %s", document_id)
            continue

            logger.info("Indexing tender %s -> %d chunks", tender_id_str, len(chunks))

        try:
            for batch_index, batch in enumerate(_batch_items(chunks, BATCH_SIZE)):
                batch_embeddings = embedder.embed(batch)
                start_index = batch_index * BATCH_SIZE

                ids = [
                    f"{document_id}_{i}"
                    for i in range(start_index, start_index + len(batch))
                ]
                metadatas = [
                    {
                        "tender_id": tender_id_str,
                        "document_id": document_id,
                        "chunk_index": i,
                        "source": source,
                        "model_name": embedder.model_name,
                        "chunk_size": CHUNK_SIZE,
                    }
                    for i in range(start_index, start_index + len(batch))
                ]

                collection.upsert(
                    ids=ids,
                    documents=batch,
                    embeddings=batch_embeddings,
                    metadatas=metadatas,
                )

            docling_outputs.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "indexed": True,
                        "indexed_at": datetime.now(timezone.utc),
                        "chunk_count": len(chunks),
                    }
                    ,
                    "$unset": {
                        "index_error": "",
                        "failed_at": "",
                    }
                }
            )
            logger.info("Indexed successfully: %s", document_id)
        except Exception as exc:
            docling_outputs.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "indexed": False,
                        "index_error": str(exc),
                        "failed_at": datetime.now(timezone.utc),
                    }
                }
            )
            logger.exception("Failed to index document %s", document_id)


def main():
    index_pending_tenders(limit=10)


if __name__ == "__main__":
    main()
