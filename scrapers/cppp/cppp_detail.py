# scrapers/cppp/cppp_detail_fetcher.py
#  Purpose:
# - Read tenders from tender_db.raw_tenders where source=CPPP
# - Open detail_url
# - Extract PDF links
# - Download PDFs locally: data/pdfs/CPPP/<tender_ref_no>/<file>.pdf
# - Store PDFs into MongoDB GridFS
# - Save metadata in tender_db.tender_documents

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from pymongo.database import Database

from utils.http import get_headers
from storage.gridfs_store import PdfGridFsStore
from storage.tender_docs_store import TenderDocumentsStore


BASE_DOMAIN = "https://etenders.gov.in"
LOCAL_PDF_ROOT = os.path.join("data", "pdfs")


def _safe_folder_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\-.() ]+", "_", name)
    name = re.sub(r"\s+", "_", name)
    return name[:120]  # avoid insane long paths on Windows


def _guess_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    base = os.path.basename(path) or "document"
    if not base.lower().endswith(".pdf"):
        base += ".pdf"
    return _safe_folder_name(base)


def _is_probably_pdf_link(href: str, text: str) -> bool:
    h = (href or "").lower()
    t = (text or "").lower()

    if ".pdf" in h:
        return True

    keywords = ["pdf", "tender document", "download", "document", "nit", "boq", "rfx", "attachment"]
    if any(k in t for k in keywords) and ("download" in h or "attachment" in h or "directlink" in h):
        return True

    return False


def extract_pdf_links(detail_html: str, detail_url: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(detail_html, "lxml")
    links = []

    for a in soup.find_all("a"):
        href = a.get("href")
        text = a.get_text(strip=True) or ""
        if not href:
            continue

        full = urljoin(BASE_DOMAIN, href) if href.startswith("/") else urljoin(detail_url, href)

        if _is_probably_pdf_link(full, text):
            links.append({
                "pdf_url": full,
                "label": text[:200]
            })

    #  Deduplicate by pdf_url
    seen = set()
    unique = []
    for x in links:
        if x["pdf_url"] in seen:
            continue
        seen.add(x["pdf_url"])
        unique.append(x)

    return unique


@dataclass
class DownloadResult:
    ok: bool
    local_path: Optional[str] = None
    gridfs_id: Optional[Any] = None
    error: Optional[str] = None


def download_pdf(
    session: requests.Session,
    pdf_url: str,
    save_dir: str
) -> DownloadResult:
    try:
        r = session.get(pdf_url, timeout=60, allow_redirects=True)
        r.raise_for_status()

        content_type = (r.headers.get("Content-Type") or "").lower()
        #  If server returns HTML, it’s likely a “needs session” or redirect page
        if "text/html" in content_type and (b"%PDF" not in r.content[:50]):
            return DownloadResult(ok=False, error=f"Got HTML instead of PDF from {pdf_url}")

        filename = _guess_filename_from_url(r.url or pdf_url)
        os.makedirs(save_dir, exist_ok=True)
        local_path = os.path.join(save_dir, filename)

        with open(local_path, "wb") as f:
            f.write(r.content)

        return DownloadResult(ok=True, local_path=local_path)

    except Exception as e:
        return DownloadResult(ok=False, error=str(e))


def fetch_cppp_pdfs_and_store(
    db: Database,
    *,
    limit: int = 50
):
    raw_tenders = db["raw_tenders"]
    tender_documents = db["tender_documents"]

    docs_store = TenderDocumentsStore(tender_documents)
    grid_store = PdfGridFsStore(db, bucket_name="pdfs")

    session = requests.Session()
    session.headers.update(get_headers())

    cursor = raw_tenders.find({"source": "CPPP"}).sort("scraped_at", -1).limit(limit)

    for t in cursor:
        source = t.get("source", "CPPP")
        tender_ref_no = t.get("tender_ref_no")
        detail_url = t.get("detail_url")

        if not tender_ref_no or not detail_url:
            continue

        print(f"\n🔎 DETAIL: {tender_ref_no}")
        try:
            resp = session.get(detail_url, timeout=60)
            resp.raise_for_status()

            pdf_links = extract_pdf_links(resp.text, detail_url)

            if not pdf_links:
                print("⚠️  No PDF links found on detail page (yet).")
                docs_store.upsert_discovered(
                    source=source,
                    tender_ref_no=tender_ref_no,
                    detail_url=detail_url,
                    pdf_links=[]
                )
                continue

            #  Save “discovered pdf links”
            docs_store.upsert_discovered(
                source=source,
                tender_ref_no=tender_ref_no,
                detail_url=detail_url,
                pdf_links=[{"pdf_url": x["pdf_url"], "label": x.get("label")} for x in pdf_links]
            )

            #  Download each PDF
            folder = os.path.join(LOCAL_PDF_ROOT, source, _safe_folder_name(tender_ref_no))

            for item in pdf_links:
                pdf_url = item["pdf_url"]

                print(f"⬇️  Downloading: {pdf_url}")
                dl = download_pdf(session, pdf_url, folder)
                if not dl.ok:
                    print(f" Failed: {dl.error}")
                    continue

                #  Put in GridFS
                with open(dl.local_path, "rb") as f:
                    data = f.read()

                filename = os.path.basename(dl.local_path)
                gridfs_id = grid_store.put_pdf(
                    source=source,
                    tender_ref_no=tender_ref_no,
                    pdf_url=pdf_url,
                    filename=filename,
                    content_type="application/pdf",
                    data=data,
                    extra_meta={"detail_url": detail_url}
                )

                print(f" Stored PDF: {filename} | GridFS: {gridfs_id}")

                docs_store.mark_downloaded(
                    source=source,
                    tender_ref_no=tender_ref_no,
                    pdf_entry={
                        "pdf_url": pdf_url,
                        "label": item.get("label"),
                        "file_name": filename,
                        "local_path": dl.local_path,
                        "gridfs_id": gridfs_id
                    }
                )

        except Exception as e:
            err = str(e)
            print(f" Error for {tender_ref_no}: {err}")
            docs_store.mark_failed(source=source, tender_ref_no=tender_ref_no, error=err)
