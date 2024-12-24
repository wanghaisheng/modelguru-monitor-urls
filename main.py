import aiohttp
import csv
import os
import asyncio
import datetime
from dotenv import load_dotenv
import sys

load_dotenv()

# Global configuration
filters = ['30_days', '7_days', '1_day', '1_year', '6_months', '3_months']
print(f"Script started by {os.getenv('GITHUB_ACTOR', 'local user')} at {datetime.datetime.utcnow().isoformat()}")

def get_time_range(filter_option):
    """
    Calculate start and end timestamps for Wayback Machine API based on filter option.
    Returns timestamps in YYYYMMDDHHMMSS format.
    """
    now = datetime.datetime.utcnow()
    
    time_ranges = {
        '30_days': 30,
        '7_days': 7,
        '1_day': 1,
        '1_year': 365,
        '6_months': 180,
        '3_months': 90
    }
    
    if filter_option not in time_ranges:
        raise ValueError(f"Invalid filter. Choose from: {', '.join(time_ranges.keys())}")
    
    start = (now - datetime.timedelta(days=time_ranges[filter_option]))
    end = now

    start_timestamp = start.strftime("%Y%m%d%H%M%S")
    end_timestamp = end.strftime("%Y%m%d%H%M%S")

    print(f"Date range: {start.isoformat()} to {end.isoformat()} UTC")
    return start_timestamp, end_timestamp

def check_environment_variables():
    """Check and validate required environment variables"""
    required_vars = {
        'DOMAIN': os.getenv('domain', 'https://www.amazon.com/sp?ie=UTF8&seller='),
        'CLOUDFLARE_API_TOKEN': os.getenv('CLOUDFLARE_API_TOKEN'),
        'CLOUDFLARE_ACCOUNT_ID': os.getenv('CLOUDFLARE_ACCOUNT_ID'),
        'CLOUDFLARE_D1_DATABASE_ID': os.getenv('CLOUDFLARE_D1_DATABASE_ID'),
        'TIME_FRAME': os.getenv('TIME_FRAME', '3')
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("\nCurrent environment variables:")
        for var, value in required_vars.items():
            masked_value = '***' if value and var not in ['DOMAIN', 'TIME_FRAME'] else value
            print(f"{var}: {masked_value}")
        sys.exit(1)

    return required_vars

async def check_url_exists(session, url, api_token, account_id, database_id):
    """Check if URL already exists in the table"""
    check_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "sql": "SELECT COUNT(*) as count FROM wayback_sellerid_data WHERE url = ?",
        "params": [url]
    }

    try:
        async with session.post(check_url, headers=headers, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('success') and data.get('result'):
                    count = data['result'][0]['count']
                    return count > 0
            return False
    except Exception as e:
        print(f"✗ Error checking URL existence: {str(e)}")
        return False

async def test_cloudflare_connection(api_token, account_id, database_id):
    """Test connection to Cloudflare API"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    print(f"\nTesting Cloudflare connection...")
    print(f"URL: {url}")
    print("Current UTC time:", datetime.datetime.utcnow().isoformat())
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                response_text = await response.text()
                print(f"Response Status: {response.status}")
                print(f"Response Body: {response_text}")
                
                if response.status == 200:
                    data = await response.json()
                    if data.get('success'):
                        print("✓ Cloudflare connection successful")
                        return True
                    else:
                        print(f"✗ Cloudflare API error: {data}")
                else:
                    print(f"✗ HTTP Status {response.status}: {response_text}")
                return False
    except Exception as e:
        print(f"✗ Connection error: {str(e)}")
        return False

async def write_to_cloudflare_d1(session, data, api_token, account_id, database_id):
    """Write data to Cloudflare D1"""
    url_exists = await check_url_exists(session, data['url'], api_token, account_id, database_id)
    
    if url_exists:
        print(f"⚠ URL already exists, skipping: {data['url']}")
        return

    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    current_time = datetime.datetime.utcnow().isoformat()
    
    payload = {
        "sql": "INSERT INTO wayback_sellerid_data (url, date, updateAt) VALUES (?, ?, ?)",
        "params": [data['url'], data['date'], current_time]
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                print(f"✓ Successfully inserted: {data['url']}")
            else:
                error_text = await response.text()
                print(f"✗ Failed to insert: {data['url']}")
                print(f"Error: {error_text}")
    except Exception as e:
        print(f"✗ Error writing to Cloudflare: {str(e)}")

async def geturls(domain, api_token, account_id, database_id, timeframe):
    """Fetch URLs from Wayback Machine and store them"""
    domainname = domain.replace("https://", "").split('/')[0]
    query_url = f"http://web.archive.org/cdx/search/cdx?url={domain}/&matchType=prefix&fl=timestamp,original&collapse=urlkey"
    
    try:
        timeframe_index = int(timeframe)
        if timeframe_index < 0 or timeframe_index >= len(filters):
            print(f"⚠ Invalid timeframe index {timeframe_index}, using default (2)")
            timeframe_index = 2
    except ValueError:
        print("⚠ Invalid timeframe value, using default (2)")
        timeframe_index = 2

    start, end = get_time_range(filters[timeframe_index])

    filter_str = f'&statuscode=200&from={start}&to={end}'
    query_url = query_url + filter_str

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
                
                os.makedirs('./result', exist_ok=True)
                
                print(f"\nProcessing {len(lines)} URLs...")
                total_processed = 0
                total_skipped = 0

                for line in lines:
                    if ' ' in line:
                        parts = line.strip().split(' ')
                        if len(parts) >= 2:
                            url=parts[1]
                            if '&seller=' in url:
                                url=url.split('&seller=')[-1]
                                if '&' in url:
                                    url=url.split('&')[0]
                            if '?seller=' in url:
                                url=url.split('?seller=')[-1]
                                if '&' in url:
                                    url=url.split('&')[0]
                            data = {
                                "url": url,
                                "date": parts[0]
                            }
                            await write_to_cloudflare_d1(session, data, api_token, account_id, database_id)
                            
                            # url_exists = await check_url_exists(session, data['url'], api_token, account_id, database_id)
                            # if url_exists:
                                # total_skipped += 1
                            # else:
                                # await write_to_cloudflare_d1(session, data, api_token, account_id, database_id)
                                # total_processed += 1

                print(f"\n✓ Processing complete:")
                print(f"  - Total URLs found: {len(lines)}")
                print(f"  - URLs processed: {total_processed}")
                print(f"  - URLs skipped (already exist): {total_skipped}")

        except Exception as e:
            print(f"✗ Error: {str(e)}")

async def create_table(api_token, account_id, database_id):
    """Create the wayback_data table if it doesn't exist"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "sql": """
        CREATE TABLE IF NOT EXISTS wayback_sellerid_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL UNIQUE,
            date TEXT NOT NULL,
            updateAt TEXT NOT NULL
        )
        """
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as response:
                if response.status == 200:
                    print("✓ Table created/verified successfully")
                else:
                    error_text = await response.text()
                    print(f"✗ Failed to create table: {error_text}")
    except Exception as e:
        print(f"✗ Error creating table: {str(e)}")

async def main():
    # Check environment variables
    env_vars = check_environment_variables()
    
    print("Starting script execution...")
    print(f"Current time (UTC): {datetime.datetime.utcnow().isoformat()}")
    
    # Test Cloudflare connection
    if not await test_cloudflare_connection(
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID']
    ):
        sys.exit(1)

    # Create table
    await create_table(
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID']
    )

    # Process URLs
    await geturls(
        env_vars['DOMAIN'],
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID'],
        env_vars['TIME_FRAME']
    )

if __name__ == "__main__":
    asyncio.run(main())
