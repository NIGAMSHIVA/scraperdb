# api/services/profile_ingest.py

from __future__ import annotations

from datetime import datetime, timezone
from bson import ObjectId
from typing import List

from docling.document_converter import DocumentConverter

from api.services.mongo import get_db
from embeddings.chunker import chunk_text
from embeddings.tender_embedder import TenderEmbedder
from embeddings.vector_store import get_chroma_collection

# ✅ Reuse your existing chunk size & batch size patterns
CHUNK_SIZE = 500
BATCH_SIZE = 64

def process_profile_job(job_id: str) -> None:
    db = get_db()
    jobs = db["jobs"]
    profiles = db["company_profiles"]
    company_docs = db["company_documents"]
    docling_outputs = db["docling_outputs"]

    job = jobs.find_one({"_id": ObjectId(job_id)})
    if not job:
        return

    profile_id = job["profile_id"]
    now = datetime.now(timezone.utc)

    # Mark job running
    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"status": "running", "step": "docling", "progress": 5, "updated_at": now}},
    )

    converter = DocumentConverter()

    # 1) Docling pending company documents
    pending_docs = list(company_docs.find({"profile_id": profile_id, "docling_status": "pending"}))

    for i, doc in enumerate(pending_docs):
        doc_id = doc["_id"]
        pdf_path = doc["local_path"]

        try:
            result = converter.convert(pdf_path)

            # Save docling output (reuse same collection, but tag doc_type=profile)
            docling_outputs.update_one(
                {"document_id": doc_id, "doc_type": "profile"},
                {
                    "$set": {
                        "doc_type": "profile",                 # ✅ important
                        "profile_id": profile_id,              # ✅ important
                        "document_id": doc_id,
                        "text": result.document.export_to_text(),
                        "tables": [],
                        "sections": None,
                        "extracted_at": datetime.utcnow(),
                        "docling_version": "v1",
                    }
                },
                upsert=True,
            )

            company_docs.update_one(
                {"_id": doc_id},
                {"$set": {"docling_status": "done", "updated_at": datetime.now(timezone.utc)}},
            )
        except Exception as e:
            company_docs.update_one(
                {"_id": doc_id},
                {"$set": {"docling_status": "failed", "updated_at": datetime.now(timezone.utc)}},
            )
            jobs.update_one(
                {"_id": ObjectId(job_id)},
                {"$set": {"status": "failed", "step": "docling", "error": str(e), "updated_at": datetime.now(timezone.utc)}},
            )
            profiles.update_one(
                {"_id": profile_id},
                {"$set": {"status": "FAILED", "updated_at": datetime.now(timezone.utc)}},
            )
            return

        # progress update (docling)
        pct = 5 + int(((i + 1) / max(1, len(pending_docs))) * 45)
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"progress": pct, "updated_at": datetime.now(timezone.utc)}},
        )

    # 2) Embed profile docling outputs into profile_embeddings
    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"step": "embedding", "progress": 55, "updated_at": datetime.now(timezone.utc)}},
    )

    profile_collection = get_chroma_collection(name="profile_embeddings")
    embedder = TenderEmbedder()

    outputs = list(docling_outputs.find({"doc_type": "profile", "profile_id": profile_id}))

    all_vectors: List[List[float]] = []
    total_chunks = 0

    for out in outputs:
        document_id = str(out["document_id"])
        text = out.get("text") or ""
        if not text.strip():
            continue

        chunks = chunk_text(text, max_chars=CHUNK_SIZE)
        if not chunks:
            continue

        # batch embed
        for b_start in range(0, len(chunks), BATCH_SIZE):
            batch = chunks[b_start:b_start + BATCH_SIZE]
            vectors = embedder.embed(batch)

            ids = [f"profile_{str(profile_id)}_{document_id}_{b_start + j}" for j in range(len(batch))]
            metadatas = [{
                "doc_type": "profile",
                "profile_id": str(profile_id),
                "document_id": document_id,
                "chunk_index": (b_start + j),
            } for j in range(len(batch))]

            profile_collection.upsert(
                ids=ids,
                documents=batch,
                embeddings=vectors,
                metadatas=metadatas
            )

            all_vectors.extend(vectors)
            total_chunks += len(batch)

        # embedding progress (rough)
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {"progress": min(95, 55 + int((total_chunks / max(1, total_chunks)) * 40)), "updated_at": datetime.now(timezone.utc)}},
        )

    # 3) Compute a single "profile embedding" for fast tender search (mean vector)
    profile_embedding = None
    if all_vectors:
        dim = len(all_vectors[0])
        mean = [0.0] * dim
        for v in all_vectors:
            for k in range(dim):
                mean[k] += float(v[k])
        mean = [x / len(all_vectors) for x in mean]
        profile_embedding = mean

    profiles.update_one(
        {"_id": profile_id},
        {"$set": {"status": "READY", "profile_embedding": profile_embedding, "updated_at": datetime.now(timezone.utc)}},
    )

    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"status": "done", "step": "ready", "progress": 100, "updated_at": datetime.now(timezone.utc)}},
    )
