import os
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

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
        soup = BeautifulSoup(response.content, "xml")  # Requires lxml
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
        run_span = soup.find("span", class_="text-r8-sm")
        if run_span:
            run_count = int(run_span.get_text(strip=True).replace("runs", "").replace('public','').strip())
            return run_count
        else:
            print(f"[WARNING] No run count found on page: {url}")
            return None
    except requests.RequestException as e:
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
        createAt = replicate_model_data.createAt; -- Preserve old createAt value
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
def main():
    print("[INFO] Starting sitemap parsing...")
    create_table_if_not_exists()

    # Parse the root sitemap
    subsitemaps = parse_sitemap(ROOT_SITEMAP_URL)
    print('detected sitemap',subsitemaps)
    if not subsitemaps:
        print("[ERROR] No subsitemaps found.")
        return

    # Process each subsitemap
    for subsitemap_url in subsitemaps:
        if subsitemap_url!='https://replicate.com/sitemap-models.xml':
            print( f'not supported yet:{subsitemap_url}')
            continue
        print(f"[INFO] Parsing subsitemap: {subsitemap_url}")
        model_urls = parse_sitemap(subsitemap_url)
        for model_url in model_urls:
            print(f"[INFO] Processing model: {model_url}")
            run_count = get_model_runs(model_url)
            if run_count is not None:
                upsert_model_data(model_url, run_count)
            time.sleep(1)  # Avoid overloading the server

    print("[INFO] Sitemap parsing complete.")

# Run the script
if __name__ == "__main__":
    main()
