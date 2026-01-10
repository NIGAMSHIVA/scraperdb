# api/services/match.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId

from api.services.mongo import get_db
from api.services.profile_ingest import build_profile_embedding_from_file
from api.services.search import search_tenders_with_embedding


def match_search(
    *,
    file_hash: str,
    file_path: str,
    query: Optional[str],
    filters: Optional[Dict[str, Any]],
    top_k: int,
) -> Optional[List[Dict[str, Any]]]:
    profile_embedding = build_profile_embedding_from_file(file_hash, file_path)
    if not profile_embedding:
        return None
    return search_tenders_with_embedding(
        profile_embedding=profile_embedding,
        top_k=top_k,
        query=query,
        filters=filters,
        profile_file_hashes=[file_hash],
    )


def process_match_job(job_id: str) -> None:
    db = get_db()
    jobs = db["jobs"]

    job = jobs.find_one({"_id": ObjectId(job_id)})
    if not job:
        return

    now = datetime.now(timezone.utc)
    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"status": "running", "step": "embedding", "progress": 10, "updated_at": now}},
    )

    file_hash = job.get("file_hash")
    file_path = job.get("file_path")
    query = job.get("query")
    filters = job.get("filters") or {}
    top_k = int(job.get("top_k") or 5)

    if not file_hash or not file_path:
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": "failed",
                    "step": "error",
                    "error": "Missing file_hash or file_path",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return

    results = match_search(
        file_hash=file_hash,
        file_path=file_path,
        query=query,
        filters=filters,
        top_k=top_k,
    )
    if results is None:
        jobs.update_one(
            {"_id": ObjectId(job_id)},
            {
                "$set": {
                    "status": "failed",
                    "step": "error",
                    "error": "Profile processing failed",
                    "updated_at": datetime.now(timezone.utc),
                }
            },
        )
        return

    jobs.update_one(
        {"_id": ObjectId(job_id)},
        {
            "$set": {
                "status": "done",
                "step": "ready",
                "progress": 100,
                "result": results,
                "updated_at": datetime.now(timezone.utc),
            }
        },
    )
