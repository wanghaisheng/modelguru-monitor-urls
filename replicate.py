import os
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

D1_DATABASE_ID = os.getenv('CLOUDFLARE_D1_DATABASE_ID')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')

# Constants
CLOUDFLARE_BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"
ROOT_SITEMAP_URL = "https://replicate.com/sitemap.xml"

HEADERS = {
    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
    "Content-Type": "application/json",
}

# Helper: Parse a sitemap and return all <loc> URLs
def parse_sitemap(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "xml")
        return [loc.text for loc in soup.find_all("loc")]
    except requests.RequestException as e:
        print(f"[ERROR] Failed to fetch sitemap {url}: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Parsing error: {e}")
        return []

# Helper: Fetch model page and extract run count
def get_model_runs(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        run_span = soup.find("ul", class_="mt-3 flex gap-4 items-center flex-wrap")
        if run_span:
            t = run_span.get_text(strip=True).lower()
            t = t.replace('public', '').replace('\n', '').strip()
            t=t.split('runs')[0].strip()
            if 'k' in t:
                t = int(float(t.replace('k', '')) * 1000)
            elif 'm' in t:
                t = int(float(t.replace('m', '')) * 1000000)

            t=re.search(r'\d+', str(t)).group(0)
            
            t = int(t)
            return t
        else:
            print(f"[WARNING] No run count found on page: {url}")
            return None
    except Exception as e:
        print(f"[ERROR] Failed to fetch model page {url}: {e}")
        return None

# Helper: Create table in the database
def create_table_if_not_exists():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS replicate_model_data (
        id SERIAL PRIMARY KEY,
        model_url TEXT UNIQUE,
        run_count INTEGER,
        createAt TEXT,
        updateAt TEXT
    );
    """
    payload = {"sql": create_table_sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        print("[INFO] Table replicate_model_data checked/created successfully.")
    except requests.RequestException as e:
        print(f"[ERROR] Failed to create table: {e}")

# Helper: Insert or update model data
def upsert_model_data(model_url, run_count):
    current_time = datetime.utcnow().isoformat()
    sql = f"""
    INSERT INTO replicate_model_data (model_url, run_count, createAt, updateAt)
    VALUES ('{model_url}', {run_count}, '{current_time}', '{current_time}')
    ON CONFLICT (model_url) DO UPDATE
    SET run_count = {run_count}, 
        updateAt = '{current_time}',
        createAt = replicate_model_data.createAt;
    """
    payload = {"sql": sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    try:
        response = requests.post(url, headers=HEADERS, json=payload)
        response.raise_for_status()
        print(f"[INFO] Data upserted for {model_url} with {run_count} runs.")
    except requests.RequestException as e:
        print(f"[ERROR] Failed to upsert data for {model_url}: {e}")

# Main workflow
def process_model_url(model_url):
    print(f"[INFO] Processing model: {model_url}")
    run_count = get_model_runs(model_url)
    if run_count is not None:
        upsert_model_data(model_url, run_count)

def main():
    print("[INFO] Starting sitemap parsing...")
    create_table_if_not_exists()

    # Parse the root sitemap
    subsitemaps = parse_sitemap(ROOT_SITEMAP_URL)
    if not subsitemaps:
        print("[ERROR] No subsitemaps found.")
        return

    for subsitemap_url in subsitemaps:
        if subsitemap_url != 'https://replicate.com/sitemap-models.xml':
            print(f"[INFO] Skipping unsupported sitemap: {subsitemap_url}")
            continue

        print(f"[INFO] Parsing subsitemap: {subsitemap_url}")
        model_urls = parse_sitemap(subsitemap_url)

        with ThreadPoolExecutor(max_workers=20) as executor:  # Adjust workers as needed
            futures = [executor.submit(process_model_url, model_url) for model_url in model_urls]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERROR] Exception during processing: {e}")

    print("[INFO] Sitemap parsing complete.")

# Run the script
if __name__ == "__main__":
    main()
