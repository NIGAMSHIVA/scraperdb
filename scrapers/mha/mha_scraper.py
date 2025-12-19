import os
import requests
import zipfile
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.mha.gov.in/en/tenders"
BASE_DOMAIN = "https://www.mha.gov.in"

PDF_DIR = "data/pdfs/MHA"
ZIP_DIR = "data/zips/MHA"
ZIP_PATH = os.path.join(ZIP_DIR, "mha_all_tenders_pdfs.zip")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120 Safari/537.36"
}


def fetch_mha_tenders():
    print("🔍 Fetching MHA tenders...")

    os.makedirs(PDF_DIR, exist_ok=True)
    os.makedirs(ZIP_DIR, exist_ok=True)

    response = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table")
    if not table:
        print("❌ Tender table not found")
        return

    rows = table.find("tbody").find_all("tr")

    pdf_files = []

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 5:
            continue

        sr_no = cols[0].get_text(strip=True)
        tender_no = cols[1].get_text(strip=True)
        title = cols[2].get_text(strip=True)
        duration = cols[4].get_text(strip=True)

        pdf_tag = cols[3].find("a", href=True)
        if not pdf_tag:
            continue

        pdf_url = urljoin(BASE_DOMAIN, pdf_tag["href"])
        pdf_name = pdf_url.split("/")[-1]
        pdf_path = os.path.join(PDF_DIR, pdf_name)

        # ---- PRINT (as requested) ----
        print("\n📄 TENDER FOUND")
        print("SR NO       :", sr_no)
        print("Tender No   :", tender_no)
        print("Title       :", title)
        print("Duration    :", duration)
        print("PDF URL     :", pdf_url)

        # ---- DOWNLOAD PDF ----
        try:
            pdf_resp = requests.get(pdf_url, headers=HEADERS, timeout=30)
            pdf_resp.raise_for_status()

            with open(pdf_path, "wb") as f:
                f.write(pdf_resp.content)

            pdf_files.append(pdf_path)
            print("✅ PDF downloaded:", pdf_name)

        except Exception as e:
            print("⚠️ Failed to download PDF:", e)

    # ---- ZIP ALL PDFs ----
    if pdf_files:
        with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in pdf_files:
                zipf.write(file, arcname=os.path.basename(file))

        print(f"\n📦 ZIP CREATED: {ZIP_PATH}")
        print(f"📄 Total PDFs: {len(pdf_files)}")

    else:
        print("⚠️ No PDFs downloaded")



