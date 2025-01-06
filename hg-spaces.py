import os
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re
import asyncio
from Sitemapper import Sitemapper
from collect_data_wayback import collect_data_wayback

# Load environment variables
load_dotenv()

D1_DATABASE_ID = os.getenv('CLOUDFLARE_D1_DATABASE_ID')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')

# Constants
CLOUDFLARE_BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"
HEADERS = {
    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
    "Content-Type": "application/json",
}

# Helper: Parse a sitemap and return all <loc> URLs
async def parse_sitemap(url):
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
async def get_model_runs(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        run_span = soup.find("button", class_="flex items-center border-l px-1.5 py-1 text-gray-400 hover:bg-gray-50 focus:bg-gray-100 focus:outline-none dark:hover:bg-gray-900 dark:focus:bg-gray-800")
        if run_span:
            t = run_span.get_text(strip=True).lower()
            if 'k' in t:
                t = int(float(t.replace('k', '')) * 1000)
            elif 'm' in t:
                t = int(float(t.replace('m', '')) * 1000000)

            t = re.search(r'\d+', str(t)).group(0)
            return int(t)
        else:
            print(f"[WARNING] No run count found on page: {url}")
            return None
    except Exception as e:
        print(f"[ERROR] Failed to fetch model page {url}: {e}")
        return None

# Helper: Create table in the database
async def create_table_if_not_exists():
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
async def upsert_model_data(model_url, run_count):
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
async def process_model_url(model_url):
    print(f"[INFO] Processing model: {model_url}")
    run_count = await get_model_runs(model_url)
    if run_count is not None:
        await upsert_model_data(model_url, run_count)

async def main():
    print("[INFO] Starting sitemap parsing...")
    await create_table_if_not_exists()
    mode_urls = []
    url_domain = 'https://huggingface.co'
    ROOT_SITEMAP_URL = f"{url_domain}/sitemap.xml"

    subsitemaps = await parse_sitemap(ROOT_SITEMAP_URL)
    if not subsitemaps:
        print("[ERROR] No subsitemaps found.")

    for subsitemap_url in subsitemaps:
        if subsitemap_url != 'https://replicate.com/sitemap-models.xml':
            print(f"[INFO] Skipping unsupported sitemap: {subsitemap_url}")
            continue

        print(f"[INFO] Parsing subsitemap: {subsitemap_url}")
        model_urls = await parse_sitemap(subsitemap_url)

    if not model_urls:
        sitemapper = Sitemapper()
        block_extensions = ['.pdf', '.md', '.jpg', '.jpeg', '.png', '.js', '.gitattributes', '.txt', '.svg', '.csv', '.json']
        max_urls = 2000

        await sitemapper.find_model_urls(url_domain, keyword="spaces", block_extensions=block_extensions, max_urls=max_urls)

    model_urls = list(set(mode_urls))
    if len(model_urls) == 0:
        print('Using Wayback Machine as fallback')
        current_date = datetime.now()
        start_date = current_date - timedelta(days=365)
        file_path = 'hg.txt'
        collect_data_wayback(
            url_domain,
            file_path,
            start_date=int(start_date.strftime('%Y%m%d')),
            end_date=int(current_date.strftime('%Y%m%d')),
            max_count=5000,
            chunk_size=4000,
            sleep=5
        )
        if os.path.exists(file_path):
            urls = open(file_path, encoding='utf8').readlines()
            model_urls = ['/'.join(u.split('/')[5:]) for u in urls]

    await asyncio.gather(*(process_model_url(model_url) for model_url in model_urls))

    print("[INFO] Sitemap parsing complete.")

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
