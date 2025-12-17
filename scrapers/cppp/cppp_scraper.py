import requests
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

from utils.http import get_headers
from storage.raw_store import save_raw_tender

BASE_URL = "https://etenders.gov.in/eprocure/app"
BASE_DOMAIN = "https://etenders.gov.in"


def clean_title(title: str) -> str:
    if "." in title:
        return title.split(".", 1)[1].strip()
    return title.strip()


def fetch_cppp_tenders():
    print("Fetching CPPP tenders...")

    response = requests.get(
        BASE_URL,
        headers=get_headers(),
        timeout=30
    )
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")

    table = soup.find("table", {"id": "activeTenders"})
    if not table:
        print(" Active Tenders table not found on CPPP page")
        return

    rows = table.find_all("tr")[1:]  # skip header

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        link_tag = cols[0].find("a")
        if not link_tag:
            continue

        title = clean_title(link_tag.get_text(strip=True))
        detail_url = urljoin(BASE_DOMAIN, link_tag["href"])

        tender_data = {
            "source": "CPPP",
            "tender_ref_no": cols[1].get_text(strip=True),
            "title": title,
            "publish_date": cols[2].get_text(strip=True),
            "deadline": cols[3].get_text(strip=True),
            "detail_url": detail_url,
            "scraped_at": datetime.utcnow(),
            "raw_html": str(row)
        }

        print("SCRAPED:", tender_data)
        save_raw_tender(tender_data)

    print(" CPPP scraping completed.")

