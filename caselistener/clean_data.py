import os
import time
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup

# -------------------------
# HTML CLEANER
# -------------------------
def clean_html(text):
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ", strip=True)

# -------------------------
# COURTLISTENER SCRAPER
# -------------------------
class CourtListenerScraper:
    BASE_URL = "https://www.courtlistener.com/api/rest/v4"

    def __init__(self, api_token):
        self.api_token = api_token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Token {api_token}",
            "User-Agent": "Mozilla/5.0 (compatible; MyScript/1.0)"
        })

    def get_dockets(self, court_id=None, limit=None):
        url = f"{self.BASE_URL}/dockets/"
        params = {}
        if court_id:
            params["court"] = court_id

        dockets = []
        print(f"Fetching dockets from {url}...")
        while url:
            if limit is not None and len(dockets) >= limit:
                break
            try:
                resp = self.session.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                dockets.extend(results)
                url = data.get("next")
                params = {}  # clear for pagination
                time.sleep(0.5)
            except requests.exceptions.RequestException as e:
                print(f"Error fetching dockets: {e}")
                break

        if limit:
            return dockets[:limit]
        return dockets

    def get_cluster(self, cluster_id):
        url = f"{self.BASE_URL}/clusters/{cluster_id}/"
        try:
            resp = self.session.get(url)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching cluster {cluster_id}: {e}")
            return None

    def get_opinion(self, opinion_id):
        url = f"{self.BASE_URL}/opinions/{opinion_id}/"
        try:
            resp = self.session.get(url)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching opinion {opinion_id}: {e}")
            return None

    @staticmethod
    def extract_id_from_url(url):
        return url.rstrip("/").split("/")[-1]

    def scrape_cases(self, court_id=None, limit=None, output_dir="court_case_texts", sleep_time=0.5):
        os.makedirs(output_dir, exist_ok=True)
        cases = []

        dockets = self.get_dockets(court_id=court_id, limit=limit)
        print(f"Found {len(dockets)} dockets. Starting cluster & opinion extraction...\n")

        for idx, docket in enumerate(dockets, 1):
            print(f"[{idx}/{len(dockets)}] {docket.get('case_name','Unknown')}")
            cluster_urls = docket.get("clusters", [])
            for cluster_url in cluster_urls:
                cluster_id = self.extract_id_from_url(cluster_url)
                cluster = self.get_cluster(cluster_id)
                if not cluster:
                    continue
                opinion_urls = cluster.get("sub_opinions", [])
                for opinion_url in opinion_urls:
                    opinion_id = self.extract_id_from_url(opinion_url)
                    opinion = self.get_opinion(opinion_id)
                    if not opinion:
                        continue

                    raw_text = opinion.get("plain_text", "")
                    if not raw_text.strip():
                        continue
                    cleaned_text = clean_html(raw_text)

                    file_name = f"opinion_{opinion_id}.txt"
                    file_path = os.path.join(output_dir, file_name)
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(cleaned_text)

                    cases.append({
                        "case_name": opinion.get("case_name"),
                        "cluster_case_name": cluster.get("case_name"),
                        "docket_id": docket.get("id"),
                        "date_filed": docket.get("date_filed"),
                        "judges": cluster.get("judges", ""),
                        "text_file_path": file_path,
                        "word_count": len(cleaned_text.split()),
                        "download_url": opinion.get("download_url", "")
                    })
                    print(f"  ✓ Opinion {opinion_id} saved")

                    time.sleep(sleep_time)

        return cases

# -------------------------
# MAIN
# -------------------------
def main():
    API_TOKEN = "d8178eca58deced23f2a3f27dcc67d869749a06a"
    COURT = "scotus"
    LIMIT = 500  # number of dockets to fetch

    print("Starting CourtListener extraction...")
    scraper = CourtListenerScraper(API_TOKEN)
    cases = scraper.scrape_cases(court_id=COURT, limit=LIMIT, output_dir="court_case_texts")

    # Save CSV & JSON
    df = pd.DataFrame(cases)
    df.to_csv("courtlistener_cases.csv", index=False)
    with open("courtlistener_cases.json", "w", encoding="utf-8") as f:
        json.dump(cases, f, indent=2, ensure_ascii=False)

    print(f"\nExtraction complete! {len(cases)} opinions saved.")
    print("Texts → court_case_texts/")

if __name__ == "__main__":
    main()
