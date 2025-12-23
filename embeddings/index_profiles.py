# embeddings/index_profiles.py

from __future__ import annotations

import logging
from typing import List, Optional
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone
import os

import numpy as np

try:
    from embeddings.chunker import chunk_text
    from embeddings.tender_embedder import TenderEmbedder
    from embeddings.vector_store import get_chroma_collection
except ModuleNotFoundError:
    from chunker import chunk_text
    from tender_embedder import TenderEmbedder
    from vector_store import get_chroma_collection

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "tender_db")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "64"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "80"))

PROFILE_COLLECTION_NAME = os.getenv("PROFILE_CHROMA_COLLECTION", "profile_embeddings")

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
docling_outputs = db["docling_outputs"]
company_profiles = db["company_profiles"]

collection = get_chroma_collection(name=PROFILE_COLLECTION_NAME)
embedder = TenderEmbedder()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")


def _combine_text_and_tables(doc: dict) -> str:
    text = doc.get("text", "") or ""
    tables = doc.get("tables", []) or []

    table_texts: List[str] = []
    for table in tables:
        if isinstance(table, dict):
            if "text" in table and isinstance(table["text"], str):
                table_texts.append(table["text"])
            else:
                for v in table.values():
                    if isinstance(v, str) and v.strip():
                        table_texts.append(v.strip())
        else:
            if isinstance(table, str) and table.strip():
                table_texts.append(table.strip())

    return "\n".join([text] + table_texts).strip()


def _batch_items(items: List[str], batch_size: int) -> List[List[str]]:
    if batch_size <= 0:
        return [items]
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _summary_embedding(chunk_embeddings: List[List[float]]) -> List[float]:
    """
    Create a single profile embedding from chunk embeddings:
    - average
    - re-normalize to unit length
    """
    arr = np.array(chunk_embeddings, dtype=np.float32)
    mean_vec = arr.mean(axis=0)
    norm = np.linalg.norm(mean_vec)
    if norm > 0:
        mean_vec = mean_vec / norm
    return mean_vec.tolist()


def index_pending_profiles(limit: int = 10) -> None:
    pending = docling_outputs.find(
        {"doc_type": "profile", "indexed": {"$ne": True}},
        limit=limit,
    )

    for doc in pending:
        document_id = str(doc["_id"])
        profile_id = doc.get("profile_id")
        profile_id_str: Optional[str] = str(profile_id) if profile_id is not None else None
        source = doc.get("source")

        if not profile_id_str:
            logger.warning("Skipping profile doc with missing profile_id: %s", document_id)
            continue

        combined_text = _combine_text_and_tables(doc)
        if not combined_text:
            logger.warning("Skipping empty profile doc: %s", document_id)
            continue

        chunks = chunk_text(combined_text, max_chars=CHUNK_SIZE, overlap_chars=CHUNK_OVERLAP)
        if not chunks:
            logger.warning("No chunks created for profile doc: %s", document_id)
            continue

        logger.info("Indexing profile %s -> %d chunks", profile_id_str, len(chunks))

        all_embeddings: List[List[float]] = []

        try:
            for batch_index, batch in enumerate(_batch_items(chunks, BATCH_SIZE)):
                batch_embeddings = embedder.embed(batch)
                if len(batch_embeddings) != len(batch):
                    raise RuntimeError("Embedding count mismatch")

                all_embeddings.extend(batch_embeddings)

                start_index = batch_index * BATCH_SIZE
                ids = [f"profile:{document_id}:{i}" for i in range(start_index, start_index + len(batch))]

                metadatas = [
                    {
                        "doc_type": "profile",
                        "profile_id": profile_id_str,
                        "document_id": document_id,
                        "chunk_index": i,
                        "source": source,
                        "model_name": embedder.model_name,
                        "chunk_size": CHUNK_SIZE,
                        "chunk_overlap": CHUNK_OVERLAP,
                    }
                    for i in range(start_index, start_index + len(batch))
                ]

                collection.upsert(ids=ids, documents=batch, embeddings=batch_embeddings, metadatas=metadatas)

            # Update docling_outputs
            docling_outputs.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "indexed": True,
                        "indexed_at": datetime.now(timezone.utc),
                        "chunk_count": len(chunks),
                        "index_model": embedder.model_name,
                    },
                    "$unset": {"index_error": "", "failed_at": ""},
                },
            )

            # Store summary embedding back into company_profiles (fast query embedding)
            summary = _summary_embedding(all_embeddings)
            company_profiles.update_one(
                {"_id": profile_id},
                {
                    "$set": {
                        "status": "READY",
                        "profile_embedding": summary,  # length 384
                        "profile_chunk_count": len(chunks),
                        "profile_embedding_model": embedder.model_name,
                        "updated_at": datetime.now(timezone.utc),
                    }
                },
            )

            logger.info("Indexed profile successfully: %s", profile_id_str)

        except Exception as exc:
            docling_outputs.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "indexed": False,
                        "index_error": str(exc),
                        "failed_at": datetime.now(timezone.utc),
                    }
                },
            )
            logger.exception("Failed to index profile doc %s", document_id)


def main():
    index_pending_profiles(limit=10)


if __name__ == "__main__":
    main()
