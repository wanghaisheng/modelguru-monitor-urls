import aiohttp
import csv
import os
import asyncio
import datetime
from dotenv import load_dotenv
import requests
import sys

load_dotenv()

def check_environment_variables():
    """Check and validate required environment variables"""
    required_vars = {
        'DOMAIN': os.getenv('domain', 'https://www.amazon.com/sp'),
        'CLOUDFLARE_API_TOKEN': os.getenv('CLOUDFLARE_API_TOKEN'),
        'CLOUDFLARE_ACCOUNT_ID': os.getenv('CLOUDFLARE_ACCOUNT_ID'),
        'CLOUDFLARE_D1_DATABASE_ID': os.getenv('CLOUDFLARE_D1_DATABASE_ID')
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("\nCurrent environment variables:")
        for var, value in required_vars.items():
            masked_value = '***' if value and var != 'DOMAIN' else value
            print(f"{var}: {masked_value}")
        sys.exit(1)

    return required_vars

async def test_cloudflare_connection(api_token, account_id, database_id):
    """Test connection to Cloudflare API"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    payload = {
        "sql": "SELECT 1"
    }
    
    print(f"\nTesting Cloudflare connection...")
    print(f"URL: {url}")
    print(f"Headers: {headers}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('success'):
                        print("✓ Cloudflare connection successful")
                        return True
                    else:
                        print(f"✗ Cloudflare API error: {data}")
                else:
                    print(f"✗ HTTP Status {response.status}: {await response.text()}")
                return False
    except Exception as e:
        print(f"✗ Connection error: {str(e)}")
        return False

async def write_to_cloudflare_d1(session, data, api_token, account_id, database_id):
    """Write data to Cloudflare D1"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/query"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    payload = {
        "sql": "INSERT INTO wayback_data (url, timestamp) VALUES (?, ?)",
        "params": [data['url'], datetime.datetime.utcnow().isoformat()]
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                print(f"✓ Successfully inserted: {data['url']}")
            else:
                print(f"✗ Failed to insert: {data['url']}")
                print(f"Error: {await response.text()}")
    except Exception as e:
        print(f"✗ Error writing to Cloudflare: {str(e)}")

async def geturls(domain, api_token, account_id, database_id):
    """Fetch URLs from Wayback Machine and store them"""
    domainname = domain.replace("https://", "").split('/')[0]
    csv_file = f'waybackmachines-{domainname}.csv'
    query_url = f"http://web.archive.org/cdx/search/cdx?url={domain}/&matchType=prefix&fl=timestamp,original&collapse=urlkey"
    
    headers = {
        'Referer': 'https://web.archive.org/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(query_url, headers=headers) as resp:
                if resp.status != 200:
                    print(f"✗ Wayback Machine API returned status {resp.status}")
                    return

                content = await resp.text()
                lines = content.splitlines()
                
                fieldnames = ['date', 'url']
                os.makedirs('./result', exist_ok=True)
                
                print(f"\nProcessing {len(lines)} URLs...")
                for line in lines:
                    if ' ' in line:
                        data = {'url': line.strip()}
                        # Write to CSV
                        # with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
                            # writer = csv.DictWriter(f, fieldnames=fieldnames)
                            # writer.writerow(data)
                        # Write to Cloudflare D1
                        await write_to_cloudflare_d1(session, data, api_token, account_id, database_id)

                print(f"\n✓ Processed {len(lines)} URLs")

        except Exception as e:
            print(f"✗ Error: {str(e)}")

async def main():
    # Check environment variables
    env_vars = check_environment_variables()
    
    # Test Cloudflare connection
    if not await test_cloudflare_connection(
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID']
    ):
        sys.exit(1)

    # Process URLs
    await geturls(
        env_vars['DOMAIN'],
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID']
    )

if __name__ == "__main__":
    asyncio.run(main())
