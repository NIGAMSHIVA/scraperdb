from storage.raw_store import collection
from scrapers.cppp.cppp_scraper import fetch_cppp_tenders
from scrapers.cppp.cppp_detail import fetch_cppp_pdfs


def main():
    # Step 1: Scrape tenders list
    fetch_cppp_tenders()

    print("\n📄 Fetching PDFs...\n")

    # Step 2: Fetch PDFs for each tender
    for tender in collection.find({"source": "CPPP"}):
        fetch_cppp_pdfs(tender)

if __name__ == "__main__":
    main()
