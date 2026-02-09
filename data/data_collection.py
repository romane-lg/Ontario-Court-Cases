import requests
import csv
import time
from datetime import datetime
import json

class CourtListenerScraper:
    """
    Scraper for CourtListener Case Law API
    Extracts opinions, clusters, dockets, and court information
    """
    
    def __init__(self, api_token):
        """
        Initialize the scraper with your API token
        
        Args:
            api_token (str): Your CourtListener API token
        """
        self.api_token = api_token
        self.base_url = "https://www.courtlistener.com/api/rest/v4"
        self.headers = {
            'Authorization': f'Token {api_token}'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_dockets(self, court_id=None, limit=None):
        """
        Fetch dockets from the API
        
        Args:
            court_id (str): Court identifier (e.g., 'scotus', 'ca9')
            limit (int): Number of results to fetch (None = fetch ALL)
            
        Returns:
            list: List of docket objects
        """
        url = f"{self.base_url}/dockets/"
        params = {}
        
        if court_id:
            params['court'] = court_id
        
        dockets = []
        
        print(f"Fetching dockets from {url}...")
        if limit is None:
            print("⚠️  NO LIMIT SET - Will fetch ALL available dockets (this may take a while!)")
        else:
            print(f"Limit set to {limit} dockets")
        
        while url:
            # Check if we've reached the limit
            if limit is not None and len(dockets) >= limit:
                break
                
            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                dockets.extend(results)
                
                print(f"Fetched {len(dockets)} dockets so far...")
                
                # Get next page
                url = data.get('next')
                params = {}  # Clear params for next request (already in URL)
                
                # Rate limiting - be nice to the API
                time.sleep(1)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching dockets: {e}")
                break
        
        # Return all dockets if no limit, otherwise return up to limit
        if limit is None:
            return dockets
        else:
            return dockets[:limit]
    
    def get_cluster(self, cluster_id):
        """
        Fetch a specific cluster by ID
        
        Args:
            cluster_id (int): Cluster ID
            
        Returns:
            dict: Cluster object
        """
        url = f"{self.base_url}/clusters/{cluster_id}/"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching cluster {cluster_id}: {e}")
            return None
    
    def get_opinion(self, opinion_id):
        """
        Fetch a specific opinion by ID
        
        Args:
            opinion_id (int): Opinion ID
            
        Returns:
            dict: Opinion object
        """
        url = f"{self.base_url}/opinions/{opinion_id}/"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching opinion {opinion_id}: {e}")
            return None
    
    def extract_cluster_id(self, cluster_url):
        """Extract cluster ID from URL"""
        return cluster_url.rstrip('/').split('/')[-1]
    
    def extract_opinion_id(self, opinion_url):
        """Extract opinion ID from URL"""
        return opinion_url.rstrip('/').split('/')[-1]
    
    def scrape_cases(self, court_id=None, limit=None):
        """
        Main scraping function - gets dockets, clusters, and opinions
        
        Args:
            court_id (str): Court identifier
            limit (int): Maximum number of cases to scrape (None = ALL)
            
        Returns:
            list: List of dictionaries containing case data
        """
        cases_data = []
        
        # Step 1: Get dockets
        print("\n=== Step 1: Fetching Dockets ===")
        dockets = self.get_dockets(court_id, limit)
        
        print(f"\nFound {len(dockets)} dockets. Now fetching clusters and opinions...\n")
        
        # Step 2: For each docket, get clusters and opinions
        for idx, docket in enumerate(dockets, 1):
            print(f"Processing docket {idx}/{len(dockets)}: {docket.get('case_name', 'Unknown')}")
            
            # Get cluster URLs from docket
            cluster_urls = docket.get('clusters', [])
            
            if not cluster_urls:
                print(f"  No clusters found for this docket")
                continue
            
            # Process each cluster
            for cluster_url in cluster_urls:
                cluster_id = self.extract_cluster_id(cluster_url)
                
                # Fetch cluster details
                cluster = self.get_cluster(cluster_id)
                if not cluster:
                    continue
                
                # Get opinion URLs from cluster
                opinion_urls = cluster.get('sub_opinions', [])
                
                # Process each opinion
                for opinion_url in opinion_urls:
                    opinion_id = self.extract_opinion_id(opinion_url)
                    
                    # Fetch opinion details
                    opinion = self.get_opinion(opinion_id)
                    if not opinion:
                        continue
                    
                    # Compile all data into one row
                    case_data = {
                        # Docket information
                        'docket_id': docket.get('id'),
                        'docket_number': docket.get('docket_number'),
                        'case_name': docket.get('case_name'),
                        'court_id': docket.get('court_id'),
                        'date_filed': docket.get('date_filed'),
                        'date_terminated': docket.get('date_terminated'),
                        'nature_of_suit': docket.get('nature_of_suit'),
                        'cause': docket.get('cause'),
                        'jurisdiction_type': docket.get('jurisdiction_type'),
                        
                        # Cluster information
                        'cluster_id': cluster.get('id'),
                        'cluster_date_filed': cluster.get('date_filed'),
                        'cluster_case_name': cluster.get('case_name'),
                        'judges': cluster.get('judges', ''),
                        'panel_str': cluster.get('panel_str', ''),
                        'citation_count': cluster.get('citation_count', 0),
                        
                        # Opinion information
                        'opinion_id': opinion.get('id'),
                        'opinion_type': opinion.get('type'),
                        'author_str': opinion.get('author_str', ''),
                        'opinion_text_html': opinion.get('html_with_citations', ''),
                        'opinion_text_plain': opinion.get('plain_text', ''),
                        'download_url': opinion.get('download_url', ''),
                        'opinions_cited_count': len(opinion.get('opinions_cited', [])),
                        
                        # URLs for reference
                        'absolute_url': docket.get('absolute_url', ''),
                        'cluster_url': f"https://www.courtlistener.com/opinion/{cluster.get('id')}/",
                    }
                    
                    cases_data.append(case_data)
                    print(f"  ✓ Extracted opinion {opinion_id} ({opinion.get('type', 'unknown type')})")
                
                # Rate limiting
                time.sleep(0.5)
        
        return cases_data
    
    def save_to_csv(self, cases_data, filename='courtlistener_cases.csv'):
        """
        Save scraped data to CSV file
        
        Args:
            cases_data (list): List of case dictionaries
            filename (str): Output filename
        """
        if not cases_data:
            print("No data to save!")
            return
        
        # Get all unique keys from all dictionaries
        fieldnames = list(cases_data[0].keys())
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(cases_data)
        
        print(f"\n✓ Successfully saved {len(cases_data)} records to {filename}")
    
    def save_to_json(self, cases_data, filename='courtlistener_cases.json'):
        """
        Save scraped data to JSON file (alternative format)
        
        Args:
            cases_data (list): List of case dictionaries
            filename (str): Output filename
        """
        if not cases_data:
            print("No data to save!")
            return
        
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(cases_data, jsonfile, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Successfully saved {len(cases_data)} records to {filename}")


def main():
    """
    Main execution function with example usage
    """
    # CONFIGURATION - EDIT THESE VALUES
    API_TOKEN = "d8178eca58deced23f2a3f27dcc67d869749a06a"  # REPLACE WITH YOUR ACTUAL TOKEN!
    
    # Search parameters
    COURT_ID = "scotus"  # Supreme Court of the United States
    # Other examples: "ca9" (9th Circuit), "dcd" (D.C. District), "nysd" (S.D.N.Y.)
    
    # Set to None to fetch ALL cases, or set a number to limit
    LIMIT = 200  # Fetch 200 cases
    
    # Initialize scraper
    scraper = CourtListenerScraper(API_TOKEN)
    
    # Scrape cases
    print("Starting CourtListener scraper...")
    print(f"Court: {COURT_ID}")
    if LIMIT is None:
        print("⚠️  WARNING: Fetching ALL cases - this could take hours!")
    else:
        print(f"Limit: {LIMIT} cases")
    print()
    
    cases = scraper.scrape_cases(
        court_id=COURT_ID,
        limit=LIMIT
    )
    
    # Save results
    if cases:
        scraper.save_to_csv(cases, 'courtlistener_cases.csv')
        scraper.save_to_json(cases, 'courtlistener_cases.json')  # Optional JSON backup
        
        print(f"\n=== Summary ===")
        print(f"Total cases extracted: {len(cases)}")
        print(f"Courts represented: {len(set(c['court_id'] for c in cases))}")
        if cases[0].get('date_filed'):
            dates = [c['date_filed'] for c in cases if c.get('date_filed')]
            if dates:
                print(f"Date range: {min(dates)} to {max(dates)}")
    else:
        print("\nNo cases found with the specified criteria.")


if __name__ == "__main__":
    main()