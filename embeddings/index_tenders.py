# embeddings/index_tenders.py

from __future__ import annotations

from typing import List
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
import os

from embeddings.chunker import chunk_text
from embeddings.tender_embedder import TenderEmbedder
from embeddings.vector_store import get_chroma_collection

load_dotenv()

# ---------------- CONFIG ---------------- #

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "tender_db"

BATCH_SIZE = 64  # safe for CPU
CHUNK_SIZE = 500

# ---------------- INIT ---------------- #

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

documents = db["tender_documents"]
docling_outputs = db["docling_outputs"]

collection = get_chroma_collection()
embedder = TenderEmbedder()


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


# ---------------- MAIN LOGIC ---------------- #

def index_pending_tenders(limit: int = 10) -> None:
    """
    Embed and index tenders that are not yet indexed.
    """

    pending = docling_outputs.find(
        {"indexed": {"$ne": True}},
        limit=limit
    )

    for doc in pending:
        document_id = str(doc["_id"])
        tender_id = doc.get("tender_id")

        combined_text = _combine_text_and_tables(doc)

        if not combined_text:
            print(f"⚠️ Skipping empty doc: {document_id}")
            continue

        chunks = chunk_text(combined_text, max_chars=CHUNK_SIZE)

        if not chunks:
            print(f"⚠️ No chunks created for: {document_id}")
            continue

        print(f"📌 Indexing tender {tender_id} → {len(chunks)} chunks")

        embeddings = embedder.embed(chunks)

        ids = [f"{document_id}_{i}" for i in range(len(chunks))]
        metadatas = [
            {
                "tender_id": tender_id,
                "document_id": document_id,
                "chunk_index": i,
            }
            for i in range(len(chunks))
        ]

        collection.add(
            ids=ids,
            documents=chunks,
            embeddings=embeddings,
            metadatas=metadatas,
        )

        # mark as indexed
        docling_outputs.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {
                    "indexed": True,
                    "indexed_at": datetime.utcnow(),
                    "chunk_count": len(chunks),
                }
            }
        )

        print("✅ Indexed successfully\n")


def main():
    index_pending_tenders(limit=10)


if __name__ == "__main__":
    main()
