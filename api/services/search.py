# api/services/search.py

from __future__ import annotations

from bson import ObjectId
from typing import Any, Dict, List
from datetime import datetime, timezone

from api.services.mongo import get_db
from embeddings.vector_store import get_chroma_collection
from embeddings.tender_embedder import TenderEmbedder

def _similarity_from_distance(distance: float) -> float:
    # For cosine distance in Chroma: similarity ≈ 1 - distance
    try:
        return max(0.0, 1.0 - float(distance))
    except Exception:
        return 0.0

def search_tenders_for_profile(profile_id: str, top_k: int = 5) -> List[Dict[str, Any]]:
    db = get_db()
    profiles = db["company_profiles"]
    tender_docs = db["tender_documents"]

    profile = profiles.find_one({"_id": ObjectId(profile_id)})
    if not profile or not profile.get("profile_embedding"):
        return []

    query_embedding = profile["profile_embedding"]

    tender_collection = get_chroma_collection(name="tender_embeddings")
    profile_collection = get_chroma_collection(name="profile_embeddings")
    embedder = TenderEmbedder()

    # 1) Query tenders using profile embedding
    res = tender_collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    documents = (res.get("documents") or [[]])[0]
    metadatas = (res.get("metadatas") or [[]])[0]
    distances = (res.get("distances") or [[]])[0]

    results: List[Dict[str, Any]] = []

    for doc_text, meta, dist in zip(documents, metadatas, distances):
        tender_document_id = meta.get("document_id")  # your tender index stores this
        tender_id = meta.get("tender_id")

        # Pull tender PDF url/path for user
        tender_pdf = None
        if tender_id:
            tender_pdf = tender_docs.find_one({"tender_id": ObjectId(tender_id)})
        if not tender_pdf and tender_document_id:
            # fallback: some people store by _id/document_id
            try:
                tender_pdf = tender_docs.find_one({"_id": ObjectId(tender_document_id)})
            except Exception:
                tender_pdf = None

        # 2) “Because…”: find best matching profile snippet for this tender snippet
        tender_snippet = (doc_text or "")[:350]

        profile_snippet = ""
        try:
            emb = embedder.embed(tender_snippet)[0]
            pr = profile_collection.query(
                query_embeddings=[emb],
                n_results=1,
                include=["documents", "metadatas", "distances"],
            )
            profile_snippet = ((pr.get("documents") or [[]])[0] or [""])[0]
            profile_snippet = (profile_snippet or "")[:350]
        except Exception:
            profile_snippet = ""

        results.append({
            "tender_id": str(tender_id) if tender_id else None,
            "score": round(_similarity_from_distance(dist), 4),
            "pdf_url": tender_pdf.get("pdf_url") if tender_pdf else None,
            "local_path": tender_pdf.get("local_path") if tender_pdf else None,
            "because": {
                "tender_snippet": tender_snippet,
                "profile_snippet": profile_snippet,
            },
        })

    return results
