# api/services/search.py

from __future__ import annotations

from bson import ObjectId
from typing import Any, Dict, List, Optional
from datetime import datetime
import math
import os
import re

from api.services.mongo import get_db
from embeddings.vector_store import get_chroma_collection
from embeddings.tender_embedder import TenderEmbedder

TENDER_CHROMA_COLLECTION = os.getenv("TENDER_CHROMA_COLLECTION", "tender_embeddings")
PROFILE_CHROMA_COLLECTION = os.getenv("PROFILE_CHROMA_COLLECTION", "company_profiles")

_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")


def _similarity_from_distance(distance: float) -> float:
    try:
        return max(0.0, 1.0 - float(distance))
    except Exception:
        return 0.0


def _tokenize(text: str) -> List[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]


def _keyword_overlap_score(query: str, text: str) -> float:
    query_tokens = set(_tokenize(query))
    if not query_tokens:
        return 0.0
    text_tokens = set(_tokenize(text))
    if not text_tokens:
        return 0.0
    overlap = len(query_tokens.intersection(text_tokens))
    return overlap / max(1, len(query_tokens))


def _normalize_vector(vec: List[float]) -> List[float]:
    norm = math.sqrt(sum(float(v) * float(v) for v in vec))
    if norm <= 0:
        return vec
    return [float(v) / norm for v in vec]


def _combine_embeddings(profile_embedding: List[float], query_embedding: List[float]) -> List[float]:
    if not profile_embedding:
        return query_embedding
    if not query_embedding:
        return profile_embedding
    dim = len(profile_embedding)
    combined = [(float(profile_embedding[i]) + float(query_embedding[i])) / 2.0 for i in range(dim)]
    return _normalize_vector(combined)


def _passes_filters(tender_doc: Optional[dict], raw_tender: Optional[dict], filters: Optional[Dict[str, Any]]) -> bool:
    if not filters:
        return True

    for key, value in filters.items():
        if value is None or value == "":
            continue
        haystack = None
        if tender_doc and key in tender_doc:
            haystack = tender_doc.get(key)
        if haystack is None and raw_tender and key in raw_tender:
            haystack = raw_tender.get(key)
        if haystack is None:
            continue
        if str(value).lower() not in str(haystack).lower():
            return False
    return True


def _select_profile_snippet(
    *,
    tender_snippet: str,
    embedder: TenderEmbedder,
    profile_collection,
    profile_file_hashes: Optional[List[str]],
) -> str:
    if not tender_snippet:
        return ""
    try:
        emb = embedder.embed(tender_snippet)[0]
        pr = profile_collection.query(
            query_embeddings=[emb],
            n_results=5,
            include=["documents", "metadatas", "distances"],
        )
        documents = (pr.get("documents") or [[]])[0]
        metadatas = (pr.get("metadatas") or [[]])[0]
        for doc_text, meta in zip(documents, metadatas):
            if not profile_file_hashes or meta.get("file_hash") in profile_file_hashes:
                return (doc_text or "")[:350]
    except Exception:
        return ""
    return ""


def search_tenders_with_embedding(
    *,
    profile_embedding: List[float],
    top_k: int = 5,
    query: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    profile_file_hashes: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    if not profile_embedding:
        return []

    db = get_db()
    tender_docs = db["tender_documents"]
    raw_tenders = db["raw_tenders"]

    embedder = TenderEmbedder()
    query_embedding: List[float] = []
    normalized_query = query or ""
    if normalized_query.strip():
        embedded = embedder.embed(normalized_query)
        if embedded:
            query_embedding = embedded[0]
        else:
            normalized_query = ""
    else:
        normalized_query = ""
    combined_embedding = _combine_embeddings(profile_embedding, query_embedding)

    tender_collection = get_chroma_collection(name=TENDER_CHROMA_COLLECTION)
    profile_collection = get_chroma_collection(name=PROFILE_CHROMA_COLLECTION)

    retrieval_k = max(50, top_k) if normalized_query else top_k

    res = tender_collection.query(
        query_embeddings=[combined_embedding],
        n_results=retrieval_k,
        include=["documents", "metadatas", "distances"],
    )

    documents = (res.get("documents") or [[]])[0]
    metadatas = (res.get("metadatas") or [[]])[0]
    distances = (res.get("distances") or [[]])[0]

    results: List[Dict[str, Any]] = []

    for doc_text, meta, dist in zip(documents, metadatas, distances):
        tender_document_id = meta.get("document_id")
        tender_id = meta.get("tender_id")

        tender_pdf = None
        if tender_id:
            try:
                tender_pdf = tender_docs.find_one({"tender_id": ObjectId(tender_id)})
            except Exception:
                tender_pdf = None
        if not tender_pdf and tender_document_id:
            try:
                tender_pdf = tender_docs.find_one({"_id": ObjectId(tender_document_id)})
            except Exception:
                tender_pdf = None

        if not tender_pdf:
            continue

        expires_at = tender_pdf.get("expires_at")
        if expires_at and expires_at <= datetime.utcnow():
            continue

        raw_tender = None
        if tender_id:
            try:
                raw_tender = raw_tenders.find_one({"_id": ObjectId(tender_id)})
            except Exception:
                raw_tender = None

        if not _passes_filters(tender_pdf, raw_tender, filters):
            continue

        tender_snippet = (doc_text or "")[:350]
        profile_snippet = _select_profile_snippet(
            tender_snippet=tender_snippet,
            embedder=embedder,
            profile_collection=profile_collection,
            profile_file_hashes=profile_file_hashes,
        )

        vector_score = round(_similarity_from_distance(dist), 4)
        rerank_score = vector_score
        if normalized_query:
            keyword_score = _keyword_overlap_score(normalized_query, tender_snippet)
            rerank_score = round((vector_score * 0.7) + (keyword_score * 0.3), 4)

        results.append(
            {
                "tender_id": str(tender_id) if tender_id else None,
                "score": vector_score,
                "rerank_score": rerank_score,
                "pdf_url": tender_pdf.get("pdf_url") if tender_pdf else None,
                "local_path": tender_pdf.get("local_path") if tender_pdf else None,
                "source": tender_pdf.get("source"),
                "title": raw_tender.get("title") if raw_tender else None,
                "tender_ref_no": raw_tender.get("tender_ref_no") if raw_tender else None,
                "duration": raw_tender.get("duration") if raw_tender else None,
                "document_id": str(tender_document_id) if tender_document_id else None,
                "chunk_index": meta.get("chunk_index"),
                "because": {
                    "tender_snippet": tender_snippet,
                    "profile_snippet": profile_snippet,
                },
            }
        )

    if normalized_query:
        results.sort(key=lambda r: r.get("rerank_score", 0.0), reverse=True)
    else:
        results.sort(key=lambda r: r.get("score", 0.0), reverse=True)

    return results[:top_k]


def search_tenders_for_profile(
    profile_id: str,
    top_k: int = 5,
    query: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    db = get_db()
    profiles = db["company_profiles"]

    profile = profiles.find_one({"_id": ObjectId(profile_id)})
    if not profile or not profile.get("profile_embedding"):
        return []

    profile_embedding = profile["profile_embedding"]
    profile_file_hashes = profile.get("file_hashes") or []

    return search_tenders_with_embedding(
        profile_embedding=profile_embedding,
        top_k=top_k,
        query=query,
        filters=filters,
        profile_file_hashes=profile_file_hashes,
    )
