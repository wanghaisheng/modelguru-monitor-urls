import aiohttp
import os
import csv
import time
import asyncio
from datetime import datetime
from aiohttp_socks import ProxyType, ProxyConnector, ChainProxyConnector
from DataRecorder import Recorder
import pandas as pd
from getbrowser import setup_chrome

# Environment Variables
D1_DATABASE_ID = os.getenv('D1_APP_DATABASE_ID')
CLOUDFLARE_ACCOUNT_ID = os.getenv('CLOUDFLARE_ACCOUNT_ID')
CLOUDFLARE_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')
CLOUDFLARE_BASE_URL = f"https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/d1/database/{D1_DATABASE_ID}"

# Constants
PROXY_URL = None
DOMAIN_LIST = [
    'https://apps.apple.com/us/charts/iphone',
    'https://apps.apple.com/us/charts/ipad'
]
RESULT_FOLDER = "./result"
OUTPUT_FOLDER = "./output"

# Initialize Browser
browser = setup_chrome()


def insert_into_d1(data):
    """
    Insert rows into the D1 database.
    """
    url = f"{CLOUDFLARE_BASE_URL}/query"
    headers = {
        "Authorization": f"Bearer {CLOUDFLARE_API_TOKEN}",
        "Content-Type": "application/json"
    }

    sql_query = "INSERT INTO ios_app_data (platform, type, cid, cname, rank, appid, icon, link, title, updateAt,country) VALUES "
    values = ", ".join([
        f"('{row['platform']}', '{row['type']}', '{row['cid']}', '{row['cname']}', {row['rank']}, '{row['appid']}', '{row['icon']}', '{row['link']}', '{row['title']}', '{row['updateAt']}','{row['country']}')"
        for row in data
    ])
    sql_query += values + ";"

    payload = {"sql": sql_query}

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print("Data inserted successfully.")
    except requests.RequestException as e:
        print(f"Failed to insert data: {e}")


def save_csv_to_d1(file_path):
    """
    Read a CSV file and insert its contents into the Cloudflare D1 database.
    """
    data = []
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        insert_into_d1(data)
    except Exception as e:
        print(f"Error reading CSV file '{file_path}': {e}")


def process_line(csv_file, lines):
    """
    Process and save lines to CSV.
    """
    for line in lines:
        try:
            line = line.strip()
            if ' ' in line:
                timestamp, original_url = line.split(' ')
                data = {'timestamp': timestamp, 'url': original_url}
                csv_file.add_data(data)
        except Exception as e:
            print(f"Failed to process line: {line}, Error: {e}")


def get_category_urls(domain):
    """
    Extract category URLs from a given domain.
    """
    try:
        tab = browser.new_tab()
        domainname = domain.replace("https://", "").replace('/', '-')
        tab.get(domain)
        print('click app or game button')
        buttons = tab.ele('.we-genre-filter__triggers-list').eles('t:button')
        csv_filepath = f'{RESULT_FOLDER}/top-app-category-{domainname}.csv'
        csv_file = Recorder(csv_filepath)

        curls = []
        for button in buttons:
            button.click()
            print('detect c url')
            appc = tab.ele('.we-genre-filter__categories-list l-content-width')
            links = appc.children()
            for a in links:
                url = a.link
                if url and 'https://apps.apple.com/us/charts' in url:
                    csv_file.add_data(url)
                    curls.append(url)

        csv_file.record()
        return curls
    except Exception as e:
        print(f"Error fetching category URLs for {domain}: {e}")
        return []


def getids_from_category(url, outfile):
    """
    Extract app details from a category URL.
    """
    full_url=None
    try:
        tab = browser.new_tab()
        cid = url.split('/')[-1]
        cname = url.split('/')[-2]
        platform = url.split('/')[-3]
        country = url.split('/')[-4]

        for chart_type in ['chart=top-free', 'chart=top-paid']:
            type = chart_type.split('-')[-1]
            full_url = f"{url}?{chart_type}"
            print('detect apps:',full_url)
            tab.get(full_url)

            links = tab.ele('.l-row chart').children()
            for link in links:
                app_link = link.ele('tag:a').link
                icon = link.ele('.we-lockup__overlay').ele('t:img').link
                rank = link.ele('.we-lockup__rank').text
                title = link.ele('.we-lockup__title').text

                outfile.add_data({
                    "platform": platform,
                    "country":country,
                    "type": type,
                    "cid": cid,
                    "cname": cname,
                    "rank": rank,
                    "appid": app_link.split('/')[-1],
                    "icon": icon,
                    "link": app_link,
                    "title": title,
                    "updateAt": datetime.now()
                })
    except Exception as e:
        print(f"Error processing category URL {full_url}: {e}")


async def get_urls_from_archive(domain, start, end):
    """
    Fetch URLs from the Wayback Machine.
    """
    try:
        domainname = domain.replace("https://", "").replace('/', '-')
        csv_filepath = f'{RESULT_FOLDER}/top-app-{domainname}.csv'
        csv_file = Recorder(csv_filepath)

        query_url = f"http://web.archive.org/cdx/search/cdx?url={domain}/&fl=timestamp,original&collapse=urlkey&matchType=prefix"
        headers = {
            'Referer': 'https://web.archive.org/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(query_url, headers=headers, timeout=3000) as resp:
                resp.raise_for_status()
                buffer = bytearray()
                while True:
                    chunk = await resp.content.read(1024)
                    if not chunk:
                        if buffer:
                            process_line(csv_file, [buffer.decode('utf-8', 'replace')])
                        break
                    buffer.extend(chunk)
    except Exception as e:
        print(f"Error fetching archive URLs for {domain}: {e}")


async def main():
    """
    Main entry point for asynchronous execution.
    """
    try:
        os.makedirs(RESULT_FOLDER, exist_ok=True)

        for domain in DOMAIN_LIST:
            print(f"Processing domain: {domain}")
            category_urls = get_category_urls(domain)
            print(f'category urls:{category_urls}')
            current_time = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            outfile_path = f'{RESULT_FOLDER}/top-100-app-{current_time}.csv'
            outfile = Recorder(outfile_path)

            for url in category_urls:
                getids_from_category(url, outfile)

            outfile.record()
            save_csv_to_d1(outfile_path)

    except Exception as e:
        print(f"Error in main execution: {e}")


if __name__ == "__main__":
    asyncio.run(main())
