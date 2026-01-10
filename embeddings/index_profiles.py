# embeddings/index_profiles.py

from __future__ import annotations

import argparse
import logging
from typing import Iterable, List, Optional
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone
import os

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

BATCH_SIZE = int(os.getenv("PROFILE_BATCH_SIZE", "64"))
CHUNK_SIZE = int(os.getenv("PROFILE_CHUNK_SIZE", "500"))
CHUNK_OVERLAP = int(os.getenv("PROFILE_CHUNK_OVERLAP", "80"))

PROFILE_COLLECTION_NAME = os.getenv("PROFILE_CHROMA_COLLECTION", "company_profiles")
PROFILE_OUTPUTS_COLLECTION = "company_profile_outputs"
PROFILE_EMBEDDINGS_COLLECTION = "company_profile_embeddings"

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]
profile_outputs = db[PROFILE_OUTPUTS_COLLECTION]
profile_embeddings = db[PROFILE_EMBEDDINGS_COLLECTION]
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


def _normalize_vector(vec: List[float]) -> List[float]:
    norm = 0.0
    for v in vec:
        norm += float(v) * float(v)
    if norm <= 0:
        return vec
    scale = norm ** 0.5
    return [float(v) / scale for v in vec]


def _mean_vectors(vectors: Iterable[List[float]]) -> Optional[List[float]]:
    vectors = list(vectors)
    if not vectors:
        return None
    dim = len(vectors[0])
    mean = [0.0] * dim
    for vec in vectors:
        for i in range(dim):
            mean[i] += float(vec[i])
    mean = [v / len(vectors) for v in mean]
    return _normalize_vector(mean)


def _update_profiles_for_file_hash(file_hash: str) -> None:
    profiles = list(company_profiles.find({"file_hashes": file_hash}))
    for profile in profiles:
        file_hashes = profile.get("file_hashes") or []
        if not file_hashes:
            continue
        summaries = list(
            profile_embeddings.find(
                {"file_hash": {"$in": file_hashes}},
                {"summary_embedding": 1},
            )
        )
        vectors = [s["summary_embedding"] for s in summaries if s.get("summary_embedding")]
        profile_embedding = _mean_vectors(vectors)
        if not profile_embedding:
            continue
        company_profiles.update_one(
            {"_id": profile["_id"]},
            {
                "$set": {
                    "status": "READY",
                    "profile_embedding": profile_embedding,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )


def index_pending_profiles(limit: int = 10) -> None:
    query = {"doc_type": "profile", "indexed": {"$ne": True}}
    if limit and limit > 0:
        pending = profile_outputs.find(query, limit=limit)
    else:
        pending = profile_outputs.find(query)

    for doc in pending:
        file_hash = doc.get("file_hash")
        if not file_hash:
            logger.warning("Skipping profile output with missing file_hash: %s", doc.get("_id"))
            continue

        combined_text = _combine_text_and_tables(doc)
        if not combined_text:
            logger.warning("Skipping empty profile output: %s", file_hash)
            continue

        chunks = chunk_text(combined_text, max_chars=CHUNK_SIZE, overlap_chars=CHUNK_OVERLAP)
        if not chunks:
            logger.warning("No chunks created for profile output: %s", file_hash)
            continue

        logger.info("Indexing profile output %s -> %d chunks", file_hash, len(chunks))

        all_embeddings: List[List[float]] = []

        try:
            for batch_index, batch in enumerate(_batch_items(chunks, BATCH_SIZE)):
                batch_embeddings = embedder.embed(batch)
                if len(batch_embeddings) != len(batch):
                    raise RuntimeError("Embedding count mismatch")

                all_embeddings.extend(batch_embeddings)

                start_index = batch_index * BATCH_SIZE
                ids = [f"profile:{file_hash}:{i}" for i in range(start_index, start_index + len(batch))]

                metadatas = [
                    {
                        "doc_type": "profile",
                        "file_hash": file_hash,
                        "chunk_index": i,
                        "model_name": embedder.model_name,
                        "chunk_size": CHUNK_SIZE,
                        "chunk_overlap": CHUNK_OVERLAP,
                    }
                    for i in range(start_index, start_index + len(batch))
                ]

                collection.upsert(ids=ids, documents=batch, embeddings=batch_embeddings, metadatas=metadatas)

            summary = _mean_vectors(all_embeddings)
            if summary:
                profile_embeddings.update_one(
                    {"file_hash": file_hash},
                    {
                        "$set": {
                            "file_hash": file_hash,
                            "summary_embedding": summary,
                            "chunk_count": len(chunks),
                            "index_model": embedder.model_name,
                            "indexed_at": datetime.now(timezone.utc),
                        }
                    },
                    upsert=True,
                )

            profile_outputs.update_one(
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

            _update_profiles_for_file_hash(file_hash)
            logger.info("Indexed profile output successfully: %s", file_hash)

        except Exception as exc:
            profile_outputs.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "indexed": False,
                        "index_error": str(exc),
                        "failed_at": datetime.now(timezone.utc),
                    }
                },
            )
            logger.exception("Failed to index profile output %s", file_hash)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()
    index_pending_profiles(limit=args.limit)


if __name__ == "__main__":
    main()
