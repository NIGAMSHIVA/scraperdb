# api/services/profile_ingest.py

from __future__ import annotations

from datetime import datetime, timezone
from bson import ObjectId
from typing import Iterable, List, Optional, Tuple
import os

from docling.document_converter import DocumentConverter

from api.services.mongo import get_db
from embeddings.chunker import chunk_text
from embeddings.tender_embedder import TenderEmbedder
from embeddings.vector_store import get_chroma_collection

PROFILE_OUTPUTS_COLLECTION = "company_profile_outputs"
PROFILE_EMBEDDINGS_COLLECTION = "company_profile_embeddings"
PROFILE_CHROMA_COLLECTION = os.getenv("PROFILE_CHROMA_COLLECTION", "company_profiles")

CHUNK_SIZE = int(os.getenv("PROFILE_CHUNK_SIZE", "500"))
CHUNK_OVERLAP = int(os.getenv("PROFILE_CHUNK_OVERLAP", "80"))
BATCH_SIZE = int(os.getenv("PROFILE_BATCH_SIZE", "64"))


def _serialize_docling_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_serialize_docling_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _serialize_docling_value(v) for k, v in value.items()}
    for attr in ("model_dump", "to_dict", "dict"):
        method = getattr(value, attr, None)
        if callable(method):
            try:
                return _serialize_docling_value(method())
            except Exception:
                break
    if hasattr(value, "__dict__"):
        return {str(k): _serialize_docling_value(v) for k, v in value.__dict__.items()}
    return str(value)


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


def _combine_text_and_tables(output: dict) -> str:
    text = output.get("text", "") or ""
    tables = output.get("tables", []) or []

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


def _ensure_profile_output(
    *,
    outputs,
    converter: DocumentConverter,
    file_hash: str,
    file_path: Optional[str],
    profile_id: Optional[ObjectId],
) -> Optional[dict]:
    existing = outputs.find_one({"file_hash": file_hash})
    if existing:
        if profile_id:
            outputs.update_one(
                {"file_hash": file_hash},
                {"$addToSet": {"profile_ids": profile_id}},
            )
        return existing

    if not file_path:
        return None

    try:
        result = converter.convert(file_path)
    except Exception:
        return None
    tables_value = getattr(result.document, "tables", None)
    sections_value = getattr(result.document, "sections", None)
    now = datetime.now(timezone.utc)

    output_doc = {
        "doc_type": "profile",
        "file_hash": file_hash,
        "text": result.document.export_to_text(),
        "tables": _serialize_docling_value(tables_value),
        "sections": _serialize_docling_value(sections_value),
        "extracted_at": now,
        "docling_version": "v1",
        "indexed": False,
        "updated_at": now,
    }

    update = {"$set": output_doc, "$setOnInsert": {"created_at": now}}
    if profile_id:
        update["$addToSet"] = {"profile_ids": profile_id}

    outputs.update_one({"file_hash": file_hash}, update, upsert=True)
    return output_doc


def _ensure_profile_embedding(
    *,
    outputs,
    embeddings_meta,
    profile_collection,
    converter: DocumentConverter,
    embedder: TenderEmbedder,
    file_hash: str,
    file_path: Optional[str],
    profile_id: Optional[ObjectId],
) -> Optional[Tuple[List[float], int]]:
    existing = embeddings_meta.find_one({"file_hash": file_hash})
    if existing and existing.get("summary_embedding"):
        if profile_id:
            embeddings_meta.update_one(
                {"file_hash": file_hash},
                {"$addToSet": {"profile_ids": profile_id}},
            )
        return existing["summary_embedding"], int(existing.get("chunk_count", 0))

    output = _ensure_profile_output(
        outputs=outputs,
        converter=converter,
        file_hash=file_hash,
        file_path=file_path,
        profile_id=profile_id,
    )
    if not output:
        return None

    combined_text = _combine_text_and_tables(output)
    if not combined_text:
        return None

    chunks = chunk_text(combined_text, max_chars=CHUNK_SIZE, overlap_chars=CHUNK_OVERLAP)
    if not chunks:
        return None

    all_vectors: List[List[float]] = []

    for batch_index, batch in enumerate(_batch_items(chunks, BATCH_SIZE)):
        vectors = embedder.embed(batch)
        if len(vectors) != len(batch):
            return None

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

        profile_collection.upsert(ids=ids, documents=batch, embeddings=vectors, metadatas=metadatas)
        all_vectors.extend(vectors)

    summary = _mean_vectors(all_vectors)
    if summary is None:
        return None

    now = datetime.now(timezone.utc)
    update = {
        "$set": {
            "file_hash": file_hash,
            "summary_embedding": summary,
            "chunk_count": len(chunks),
            "index_model": embedder.model_name,
            "indexed_at": now,
            "updated_at": now,
        }
    }
    if profile_id:
        update["$addToSet"] = {"profile_ids": profile_id}

    embeddings_meta.update_one({"file_hash": file_hash}, update, upsert=True)
    outputs.update_one(
        {"file_hash": file_hash},
        {"$set": {"indexed": True, "indexed_at": now}},
    )

    return summary, len(chunks)


def build_profile_embedding_from_file(file_hash: str, file_path: str) -> Optional[List[float]]:
    db = get_db()
    outputs = db[PROFILE_OUTPUTS_COLLECTION]
    embeddings_meta = db[PROFILE_EMBEDDINGS_COLLECTION]
    converter = DocumentConverter()
    embedder = TenderEmbedder()
    profile_collection = get_chroma_collection(name=PROFILE_CHROMA_COLLECTION)

    result = _ensure_profile_embedding(
        outputs=outputs,
        embeddings_meta=embeddings_meta,
        profile_collection=profile_collection,
        converter=converter,
        embedder=embedder,
        file_hash=file_hash,
        file_path=file_path,
        profile_id=None,
    )
    if not result:
        return None
    summary, _chunk_count = result
    return summary


def process_profile_job(job_id: str) -> None:
    db = get_db()
    jobs = db["jobs"]
    profiles = db["company_profiles"]
    company_docs = db["company_documents"]
    outputs = db[PROFILE_OUTPUTS_COLLECTION]
    embeddings_meta = db[PROFILE_EMBEDDINGS_COLLECTION]

    job = jobs.find_one({"_id": ObjectId(job_id)})
    if not job:
        return

    profile_id = job["profile_id"]
    now = datetime.now(timezone.utc)

    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"status": "running", "step": "docling", "progress": 5, "updated_at": now}},
    )

    converter = DocumentConverter()
    embedder = TenderEmbedder()
    profile_collection = get_chroma_collection(name=PROFILE_CHROMA_COLLECTION)

    docs = list(company_docs.find({"profile_id": profile_id, "docling_status": {"$ne": "failed"}}))
    if not docs:
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"status": "failed", "step": "docling", "error": "No documents", "updated_at": now}},
        )
        profiles.update_one(
            {"_id": profile_id},
            {"$set": {"status": "FAILED", "updated_at": now}},
        )
        return

    summary_vectors: List[List[float]] = []
    file_hashes: List[str] = []

    for i, doc in enumerate(docs):
        doc_id = doc["_id"]
        file_hash = doc.get("file_hash")
        file_path = doc.get("local_path")

        if not file_hash:
            company_docs.update_one(
                {"_id": doc_id},
                {"$set": {"docling_status": "failed", "updated_at": datetime.now(timezone.utc)}},
            )
            jobs.update_one(
                {"_id": ObjectId(job_id)},
                {"$set": {"status": "failed", "step": "docling", "error": "Missing file hash", "updated_at": now}},
            )
            profiles.update_one(
                {"_id": profile_id},
                {"$set": {"status": "FAILED", "updated_at": now}},
            )
            return

        result = _ensure_profile_embedding(
            outputs=outputs,
            embeddings_meta=embeddings_meta,
            profile_collection=profile_collection,
            converter=converter,
            embedder=embedder,
            file_hash=file_hash,
            file_path=file_path,
            profile_id=profile_id,
        )

        if not result:
            company_docs.update_one(
                {"_id": doc_id},
                {"$set": {"docling_status": "failed", "updated_at": datetime.now(timezone.utc)}},
            )
            jobs.update_one(
                {"_id": ObjectId(job_id)},
                {"$set": {"status": "failed", "step": "embedding", "error": "Profile embedding failed", "updated_at": now}},
            )
            profiles.update_one(
                {"_id": profile_id},
                {"$set": {"status": "FAILED", "updated_at": now}},
            )
            return

        summary, _chunk_count = result
        summary_vectors.append(summary)
        file_hashes.append(file_hash)

        company_docs.update_one(
            {"_id": doc_id},
            {"$set": {"docling_status": "done", "updated_at": datetime.now(timezone.utc)}},
        )

        pct = 5 + int(((i + 1) / max(1, len(docs))) * 75)
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"progress": pct, "updated_at": datetime.now(timezone.utc)}},
        )

    profile_embedding = _mean_vectors(summary_vectors)
    if profile_embedding is None:
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"status": "failed", "step": "embedding", "error": "No embeddings", "updated_at": now}},
        )
        profiles.update_one(
            {"_id": profile_id},
            {"$set": {"status": "FAILED", "updated_at": datetime.now(timezone.utc)}},
        )
        return

    profiles.update_one(
        {"_id": profile_id},
        {
            "$set": {
                "status": "READY",
                "profile_embedding": profile_embedding,
                "file_hashes": list(dict.fromkeys(file_hashes)),
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )

    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"status": "done", "step": "ready", "progress": 100, "updated_at": datetime.now(timezone.utc)}},
    )
