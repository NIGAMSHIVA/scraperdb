# api/routes/profiles.py

from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import List, Optional
from datetime import datetime, timezone
import os
from bson import ObjectId

from api.services.mongo import get_db
from api.services.jobs import enqueue_job
from api.services.search import search_tenders_for_profile

router = APIRouter()

@router.post("/profiles")
def create_profile():
    db = get_db()
    profiles = db["company_profiles"]

    now = datetime.now(timezone.utc)
    profile = {
        "status": "UPLOADING",
        "profile_embedding": None,
        "created_at": now,
        "updated_at": now,
    }
    res = profiles.insert_one(profile)
    return {"profile_id": str(res.inserted_id)}

@router.post("/profiles/{profile_id}/documents")
async def upload_profile_documents(profile_id: str, pdfs: List[UploadFile] = File(...)):
    db = get_db()
    profiles = db["company_profiles"]
    docs = db["company_documents"]
    jobs = db["jobs"]

    # Validate profile_id
    try:
        profile_oid = ObjectId(profile_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid profile_id")

    profile = profiles.find_one({"_id": profile_oid})
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    # Save PDFs to disk
    base_dir = os.path.join("data", "pdfs", "PROFILES", profile_id)
    os.makedirs(base_dir, exist_ok=True)

    now = datetime.now(timezone.utc)

    inserted_doc_ids = []
    for f in pdfs:
        if not f.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"Only PDF allowed: {f.filename}")

        save_path = os.path.join(base_dir, f.filename)

        content = await f.read()
        with open(save_path, "wb") as out:
            out.write(content)

        doc_rec = {
            "profile_id": profile_oid,
            "document_name": f.filename,
            "local_path": save_path,
            "docling_status": "pending",
            "size_kb": round(len(content) / 1024, 2),
            "created_at": now,
            "updated_at": now,
        }
        r = docs.insert_one(doc_rec)
        inserted_doc_ids.append(r.inserted_id)

    # Create job
    job = {
        "profile_id": profile_oid,
        "status": "queued",
        "step": "docling",
        "progress": 0,
        "error": None,
        "created_at": now,
        "updated_at": now,
    }
    job_res = jobs.insert_one(job)
    job_id = str(job_res.inserted_id)

    # Update profile status
    profiles.update_one({"_id": profile_oid}, {"$set": {"status": "PROCESSING", "updated_at": now}})

    # Enqueue job (async processing)
    enqueue_job(job_id)

    return {"job_id": job_id, "profile_id": profile_id, "uploaded": len(pdfs)}

@router.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    db = get_db()
    jobs = db["jobs"]
    try:
        job_oid = ObjectId(job_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid job_id")

    job = jobs.find_one({"_id": job_oid})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return {
        "job_id": job_id,
        "profile_id": str(job["profile_id"]),
        "status": job["status"],
        "step": job.get("step"),
        "progress": job.get("progress", 0),
        "error": job.get("error"),
    }

@router.get("/profiles/{profile_id}")
def get_profile(profile_id: str):
    db = get_db()
    profiles = db["company_profiles"]
    try:
        profile_oid = ObjectId(profile_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid profile_id")

    profile = profiles.find_one({"_id": profile_oid})
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    return {"profile_id": profile_id, "status": profile["status"]}

@router.post("/profiles/{profile_id}/search")
def search(profile_id: str, top_k: Optional[int] = 5):
    db = get_db()
    profiles = db["company_profiles"]

    try:
        profile_oid = ObjectId(profile_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid profile_id")

    profile = profiles.find_one({"_id": profile_oid})
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")

    if profile.get("status") != "READY":
        raise HTTPException(status_code=409, detail="Profile not READY yet. Please wait for processing.")

    results = search_tenders_for_profile(profile_id=profile_id, top_k=int(top_k or 5))
    return {"profile_id": profile_id, "results": results}
