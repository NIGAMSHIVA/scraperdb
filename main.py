from scrapers.mha.mha_scraper import fetch_mha_tenders


def main():
    print("\n Starting MHA Tender Scraper\n")
    fetch_mha_tenders()
    print("\n MHA Scraping Completed\n")


if __name__ == "__main__":
    main()
