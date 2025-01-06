import os
import requests
import asyncio
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re
import aiohttp
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

# Concurrency limit
SEM_LIMIT = 100

# Helper: Parse a sitemap and return all <loc> URLs
async def parse_sitemap(session, url):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), "xml")
            return [loc.text for loc in soup.find_all("loc")]
    except Exception as e:
        print(f"[ERROR] Failed to fetch sitemap {url}: {e}")
        return []

# Helper: Fetch model page and extract run count
async def get_model_runs(session, url):
    try:
        # https://huggingface.co/spaces/AP123/IllusionDiffusion/discussions/94
        async with session.get(url) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), "html.parser")
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
async def create_table_if_not_exists(session):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS huggingface_spaces_data (
        id SERIAL PRIMARY KEY,
        model_url TEXT UNIQUE,
        run_count INTEGER,
        createAt TEXT,
        updateAt TEXT
    );
    """
    payload = {"sql": create_table_sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    async with session.post(url, headers=HEADERS, json=payload) as response:
        response.raise_for_status()
        print("[INFO] Table huggingface_spaces_data checked/created successfully.")

# Helper: Insert or update model data
async def upsert_model_data(session, model_url, run_count):
    current_time = datetime.utcnow().isoformat()
    sql = f"""
    INSERT INTO huggingface_spaces_data (model_url, run_count, createAt, updateAt)
    VALUES ('{model_url}', {run_count}, '{current_time}', '{current_time}')
    ON CONFLICT (model_url) DO UPDATE
    SET run_count = {run_count}, 
        updateAt = '{current_time}',
        createAt = huggingface_spaces_data.createAt;
    """
    payload = {"sql": sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    async with session.post(url, headers=HEADERS, json=payload) as response:
        response.raise_for_status()
        print(f"[INFO] Data upserted for {model_url} with {run_count} runs.")

# Process a single model URL
async def process_model_url(semaphore, session, model_url):
    async with semaphore:
        print(f"[INFO] Processing model: {model_url}")
        run_count = await get_model_runs(session, model_url)
        if run_count is not None:
            await upsert_model_data(session, model_url, run_count)

# Main function
async def main():
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    timeout = ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print("[INFO] Starting sitemap parsing...")
        await create_table_if_not_exists(session)
        
        url_domain = 'https://huggingface.co'
        ROOT_SITEMAP_URL = f"{url_domain}/sitemap.xml"
        model_urls=[]
        model_urls = await parse_sitemap(session, ROOT_SITEMAP_URL)

        model_urls = list(set(model_urls))
        if not model_urls:
            print('Using Wayback Machine as fallback')
            current_date = datetime.now()
            start_date = current_date - timedelta(days=365)
            file_path = 'hg.txt'
            model_urls=collect_data_wayback(
                url_domain+'/spaces/',
                file_path,
                start_date=int(start_date.strftime('%Y%m%d')),
                end_date=int(current_date.strftime('%Y%m%d')),
                max_count=5000,
                chunk_size=4000,
                sleep=5
            )
            # if os.path.exists(file_path):
                # with open(file_path, encoding='utf8') as f:
                    # model_urls = [line.strip() for line in f]
        print('model_urls',len(model_urls))
        baseUrl='https://huggingface.co/spaces/'
        if len(model_urls)<1:
            retrun 
        cleanurls=[]
        print('start clean url')
        for url in model_urls:

            modelname=url.replace(baseUrl,'').split('/')
            if len(modelname)<2:
                continue

            url=baseUrl+modelname[0]+'/'+modelname[1]
            cleanurls.append(url)
        model_urls=list(set(cleanurls))
        print('cleanurls',len(model_urls))
        
        await asyncio.gather(*(process_model_url(semaphore, session, url) for url in model_urls))

        print("[INFO] Sitemap parsing complete.")

if __name__ == "__main__":
    asyncio.run(main())
