import aiohttp
import csv
import os
import asyncio
import datetime
import json
import cloudflare
from dotenv import load_dotenv
from cloudflare import Cloudflare

load_dotenv()

# Load environment variables
domain = os.getenv('domain')
if domain is None:
    domain = 'https://www.amazon.com/sp'
proxy_url = os.getenv('proxy_url')
api_token = os.getenv('CLOUDFLARE_API_TOKEN')
account_id = os.getenv('CLOUDFLARE_ACCOUNT_ID')
database_id = os.getenv('CLOUDFLARE_D1_DATABASE_ID')

os.makedirs('./result', exist_ok=True)

async def geturls(domain):
    no_subs = None
    subs_wildcard = "*." if not no_subs else ""
    domainname = domain.replace("https://", "")
    domainname = domainname.split('/')[0]
    csv_file = f'waybackmachines-{domainname}.csv'
    date_today = datetime.date.today().strftime("%Y-%m-%d")

    query_url = f"http://web.archive.org/cdx/search/cdx?url={domain}/&matchType=prefix&fl=timestamp,original"
    filter = "&collapse=urlkey"
    query_url = query_url + filter

    headers = {'Referer': 'https://web.archive.org/',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/92.0.4515.107 Safari/537.36'}

    async with aiohttp.ClientSession(connector=None) as session:
        try:
            resp = await session.get(query_url, headers=headers, timeout=300000)
            count = 0
            while True:
                raw = await resp.content.read(10240)
                if not raw:
                    break
                try:
                    raw = raw.decode('utf-8')
                except UnicodeDecodeError:
                    raw = raw.decode('latin-1')

                if resp.status != 200:
                    print('not 200')
                lines = raw.splitlines()
                fieldnames = ['date', 'url']
                count += len(lines)
                file_exists = os.path.isfile(csv_file)

                for line in lines:
                    if ' ' in line:
                        data = {'url': line.strip()}
                        with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            writer.writerow(data)
                        # Write to Cloudflare D1 table
                        await write_to_cloudflare_d1(data)

            print('============', count)

        except aiohttp.ClientError as e:
            print(f"Connection error: {e}", 'red')
        except Exception as e:
            print(f"Couldn't get list of responses: {e}", 'red')

async def write_to_cloudflare_d1(data):
    cf = Cloudflare(api_token)
    try:
        response = cf.accounts.dns_records.post(
            account_id,
            database_id,
            json={
                "query": "INSERT INTO wayback_data (url) VALUES ($1)",
                "params": [data['url']]
            }
        )
        if response.status_code == 200:
            print(f"Successfully inserted data: {data}")
        else:
            print(f"Failed to insert data: {response.content}")
    except Exception as e:
        print(f"Error writing to Cloudflare D1: {e}")

asyncio.run(geturls(domain))
