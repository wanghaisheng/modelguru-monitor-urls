import os
import aiohttp
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv
import re

# Load environment variables
load_dotenv()

D1_DATABASE_ID = os.getenv('CLOUDFLARE_D1_DATABASE_ID')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')

# Constants
CLOUDFLARE_BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"
ROOT_SITEMAP_URL = "https://civitai.com/sitemap.xml"

HEADERS = {
    "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
    "Content-Type": "application/json",
}

# Semaphore for controlling concurrency
MAX_CONCURRENT_REQUESTS = 50  # Adjust based on system capabilities
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# Helper: Parse a sitemap and return all <loc> URLs
async def parse_sitemap(url, session):
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                soup = BeautifulSoup(text, "xml")
                return [loc.text for loc in soup.find_all("loc")]
        except aiohttp.ClientError as e:
            print(f"[ERROR] Failed to fetch sitemap {url}: {e}")
            return []

# Helper: Fetch model page and extract run count
async def get_model_runs(url, session):
    stats=[]
    
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                run_spans = soup.find_all("tr", class_="mantine-1avyp1d")

                
                if run_spans and len(run_spans) > 2:
                    td = run_spans[1]
                    spans=td.find_all("span",class_="mantine-h9iq4m mantine-Badge-inner")
                    for run_span in spans:
                        
                        t = run_span.get_text(strip=True).lower()
                        t=t.replace('stats','').strip()
                        if 'k' in t:
                            t = int(float(t.replace('k', '')) * 1000)
                        elif 'm' in t:
                            t = int(float(t.replace('m', '')) * 1000000)
                        if ',' in t:
                            t = t.replace(',', '')
                            
                        t = int(t)
                        stats.append(t)
                    print('stats', stats)
                    
                    return stats
                else:
                    print(f"[WARNING] No run count found on page: {url}")
                    return stats
        except aiohttp.ClientError as e:
            print(f"[ERROR] Failed to fetch model page {url}: {e}")
            return stats

# Helper: Create table in the database
async def create_table_if_not_exists(session):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS civitai_model_data (
        id SERIAL PRIMARY KEY,
        model_url TEXT UNIQUE,
        download_count INTEGER,
        run_count INTEGER,
        type TEXT,
        createAt TEXT,
        updateAt TEXT
    );
    """
    payload = {"sql": create_table_sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    try:
        async with session.post(url, headers=HEADERS, json=payload) as response:
            response.raise_for_status()
            print("[INFO] Table civitai_model_data checked/created successfully.")
    except aiohttp.ClientError as e:
        print(f"[ERROR] Failed to create table: {e}")

# Helper: Insert or update model data
async def upsert_model_data(model_url, stats, type, session):
    current_time = datetime.utcnow().isoformat()
    download_count=stats[0]
    run_count=stats[1]
    sql = f"""
    INSERT INTO civitai_model_data (model_url, download_count,run_count, type, createAt, updateAt)
    VALUES ('{model_url}',{download_count}, {run_count}, '{type}', '{current_time}', '{current_time}')
    ON CONFLICT (model_url) DO UPDATE
    SET run_count = {run_count}, 
        updateAt = '{current_time}',
        createAt = civitai_model_data.createAt;
    """
    payload = {"sql": sql}
    
    url = f"{CLOUDFLARE_BASE_URL}/query"
    try:
        async with session.post(url, headers=HEADERS, json=payload) as response:
            response.raise_for_status()
            print(f"[INFO] Data upserted for {model_url} with {run_count} runs.")
    except aiohttp.ClientError as e:
        print('insert data',sql)
        
        print(f"[ERROR] Failed to upsert data for {model_url}: {e}")

# Main workflow
async def process_model_url(model_url, type, session):
    print(f"[INFO] Processing model: {model_url}")
    stats = await get_model_runs(model_url, session)
    if stats is not None and len(stats)==2:
        await upsert_model_data(model_url, stats, type, session)

async def main():
    print("[INFO] Starting sitemap parsing...")
    async with aiohttp.ClientSession() as session:
        await create_table_if_not_exists(session)

        # Parse the root sitemap
        subsitemaps = await parse_sitemap(ROOT_SITEMAP_URL, session)
        if not subsitemaps:
            print("[ERROR] No subsitemaps found.")
            return

        tasks = []
        for subsitemap_url in subsitemaps:
            type = subsitemap_url.replace('https://civitai.com/sitemap-', '').replace('.xml', '')
            if len(type) == 1 or type != 'models':
                continue
            print(f"[INFO] Parsing subsitemap: {subsitemap_url}")
            model_urls = await parse_sitemap(subsitemap_url, session)

            for model_url in model_urls[:10]:
                tasks.append(process_model_url(model_url, type, session))

        await asyncio.gather(*tasks)
    print("[INFO] Sitemap parsing complete.")

# Run the script
if __name__ == "__main__":
    asyncio.run(main())
