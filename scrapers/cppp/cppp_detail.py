import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from utils.http import get_headers
from storage.pdf_store import save_pdf_metadata

BASE_DOMAIN = "https://etenders.gov.in"

def fetch_cppp_pdfs(tender):
    print(f"🔎 DETAIL: {tender['tender_ref_no']}")

    r = requests.get(
        tender["detail_url"],
        headers=get_headers(),
        timeout=30
    )

    soup = BeautifulSoup(r.text, "lxml")

    tables = soup.find_all("table")
    pdf_found = False

    for table in tables:
        for a in table.find_all("a"):
            href = a.get("href", "")
            if "DownloadTender" in href or href.endswith((".pdf", ".xls", ".xlsx")):
                pdf_found = True

                file_url = urljoin(BASE_DOMAIN, href)
                file_name = a.get_text(strip=True)

                save_and_download_pdf(
                    tender,
                    file_url,
                    file_name
                )

    if not pdf_found:
        print("⚠️  No PDF links found (table exists but empty)")

def save_and_download_pdf(tender, url, file_name):
    folder = f"data/pdfs/CPPP/{tender['tender_ref_no']}"
    os.makedirs(folder, exist_ok=True)

    local_path = os.path.join(folder, file_name)

    r = requests.get(url, headers=get_headers(), timeout=30)

    with open(local_path, "wb") as f:
        f.write(r.content)

    save_pdf_metadata({
        "tender_ref_no": tender["tender_ref_no"],
        "source": tender["source"],
        "document_name": file_name,
        "document_type": "UNKNOWN",
        "local_path": local_path,
        "size_kb": round(len(r.content) / 1024, 2)
    })

    print(f"📄 Saved: {file_name}")
