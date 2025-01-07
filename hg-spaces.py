import os
import requests
import asyncio
from aiohttp import ClientSession, ClientTimeout
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dotenv import load_dotenv
import re
import aiohttp
from collect_data_wayback import collect_data_wayback,exact_url_timestamp
from waybackpy import WaybackMachineCDXServerAPI
import cdx_toolkit
from domainLatestUrl import DomainMonitor
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
ccisopen=False

# Concurrency limit
SEM_LIMIT = 20

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
async def get_model_runs(session, item):
    try:
        url=item.get('model_url')
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
                item['run_count']=t
                return item
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
        google_indexAt TEXT,
        wayback_createAt TEXT,
        cc_createAt TEXT,
        updateAt TEXT
    );
    """
    payload = {"sql": create_table_sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"
    async with session.post(url, headers=HEADERS, json=payload) as response:
        response.raise_for_status()
        result = await response.json()
        
        if result.get("success"):
            print("[INFO] Table huggingface_spaces_data checked/created successfully.")
            return True  # Assuming table creation was successful
        return False  # Assuming table already existed
        


# Helper: Insert or update model data with retry and exception handling
async def get_model_date(session, item, max_retries=3, retry_delay=5):
    current_time = datetime.utcnow().isoformat()
    model_url=item.get('model_url')
    print('Try to find first index date of', model_url)
    user_agent = "check huggingface model's user agent"
    wayback_createAt = None
    cc_createAt = None    

    # try:
        # cdx_api = WaybackMachineCDXServerAPI(model_url, user_agent)
        # oldest = cdx_api.oldest()
        # if oldest.datetime_timestamp:
            # wayback_createAt = oldest.datetime_timestamp.isoformat()
        # print('==WaybackMachineCDXServerAPI=', wayback_createAt)
    # except Exception as e:
        # print('WaybackMachineCDXServerAPI failed:', e)

    current_date = datetime.now()
    start_date = current_date - timedelta(days=365)
    start_date = int(start_date.strftime('%Y%m%d'))
    current_date = int(current_date.strftime('%Y%m%d'))
    wayback_createAt=exact_url_timestamp(model_url)
    # for t in ['cc','ia']:
        # if ccisopen==False and t=='cc':
            # continue
        # try:
            # cdx = cdx_toolkit.CDXFetcher(source=t)
            # for obj in cdx.iter(model_url, from_ts=start_date, to=current_date,limit=1, cc_sort='ascending'):
                # if t=='cc':
                
                    # cc_createAt = obj.get('timestamp')
                # if t=='ia':
                    # wayback_createAt = obj.get('timestamp')
                
        # except Exception as e:
            # print('t failed:', e)
    item['wayback_createAt']=wayback_createAt
    item['cc_createAt']=cc_createAt

async def upsert_model_data(session, max_retries=3, retry_delay=5):
    model_url=item.get('model_url')
    run_count=item.get('run_count')
    google_indexAt=item.get('google_indexAt',None)
    wayback_createAt=item.get('wayback_createAt',None)
    cc_createAt=item.get('cc_createAt',None)
    
    sql = f"""
    INSERT INTO huggingface_spaces_data (model_url, run_count, google_indexAt,wayback_createAt, cc_createAt, updateAt)
    VALUES ('{model_url}', {run_count}, 
    
            {f"'{google_indexAt}'" if google_indexAt else 'NULL'}, 
            {f"'{wayback_createAt}'" if wayback_createAt else 'NULL'}, 
            {f"'{cc_createAt}'" if cc_createAt else 'NULL'}, 
            '{current_time}')
    ON CONFLICT (model_url) DO UPDATE
    SET run_count = {run_count}, 
        updateAt = '{current_time}',
        google_indexAt = COALESCE(huggingface_spaces_data.google_indexAt, EXCLUDED.google_indexAt),
        wayback_createAt = COALESCE(huggingface_spaces_data.wayback_createAt, EXCLUDED.wayback_createAt),
        cc_createAt = COALESCE(huggingface_spaces_data.cc_createAt, EXCLUDED.cc_createAt);
    """
    payload = {"sql": sql}
    url = f"{CLOUDFLARE_BASE_URL}/query"

    for attempt in range(max_retries):
        try:
            async with session.post(url, headers=HEADERS, json=payload) as response:
                response.raise_for_status()
                print(f"[INFO] Data upserted for {model_url} with {run_count} runs.")
                return
        except aiohttp.ClientError as e:
            print(f"[ERROR] Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
        except Exception as e:
            print(f"[ERROR] Unexpected error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                print(f"[INFO] Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
    print(f"[ERROR] Failed to upsert data for {model_url} after {max_retries} attempts.")

# Process a single model URL
async def process_model_url(semaphore, session, item):
    async with semaphore:
        model_url=item.get("model_url")
        print(f"[INFO] Processing model: {model_url}")
        item = await get_model_runs(session, item)
        print(f"[INFO] save statics: {run_count}")
        
        if item is not None:
            await upsert_model_data(session, item)

# Main function
async def main():
    semaphore = asyncio.Semaphore(SEM_LIMIT)
    timeout = ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        print("[INFO] Starting sitemap parsing...")
        tablenewcreate=await create_table_if_not_exists(session)
        if tablenewcreate:
            print('Using Wayback Machine as fallback')
            current_date = datetime.now()
            start_date = current_date - timedelta(days=365)
            file_path = 'hg.txt'
            items=exact_url_timestamp(
                url_domain+'/spaces/',
                max_count=5000,
                chunk_size=1000,
                sleep=5
            )
            # if os.path.exists(file_path):
                # with open(file_path, encoding='utf8') as f:
                    # model_urls = [line.strip() for line in f]
            print('items',len(items))
            print("[INFO] wayback check parsing complete.")
            unique_items = {}

            baseUrl='https://huggingface.co/spaces/'
            if len(items)<1:
                return 
            cleanitems=[]
            print('start clean url')
            uniqueurls=[]
            for item in items:
                url=item.get('url')
                if '?' in url:
                    url=url.split('?')[0]
                modelname=url.replace(baseUrl,'').split('/')
                if len(modelname)<2:
                    continue

                url=baseUrl+modelname[0]+'/'+modelname[1]
                if url in unique_items:
                
                    existing_item = unique_items[url]
                    
                    existing_wayback_createAt = existing_item.get('wayback_createAt')
                    if wayback_createAt < existing_wayback_createAt:
                        existing_item['wayback_createAt'] = wayback_createAt
    
                else:
                    item['model_url'] = url
                    item['wayback_createAt'] = wayback_createAt
                    unique_items[url] = item
            cleanitems = list(unique_items.values())

            print('cleanitems',len(cleanitems))
            await asyncio.gather(*(process_model_url(semaphore, session, item) for item in cleanitems))
        else:
            url_domain = 'https://huggingface.co'
            ROOT_SITEMAP_URL = f"{url_domain}/sitemap.xml"
            model_urls=[]
            model_urls = await parse_sitemap(session, ROOT_SITEMAP_URL)
            print("[INFO] Sitemap parsing complete.")
            model_urls = list(set(model_urls))
       
            d=DomainMonitor()
            search_model_urls=[]
            results=d.monitor_site(site=baseUrl,time_range='24h')
            if len(results)>1:
                for r in results:
                    search_model_urls.append(r.get('url'))
            search_model_urls=list(set(search_model_urls))
            print("[INFO] google search check  complete.")
            gindex=int(datetime.now().strftime('%Y%m%d'))
            items=[]
            for i in search_model_urls:
                item={}
                item['model_url']=url
                item['google_indexAt']=gindex
                items.append(item)
            await asyncio.gather(*(process_model_url(semaphore, session, item) for item in items))

        print("[INFO] url detect complete.")


if __name__ == "__main__":
    asyncio.run(main())
