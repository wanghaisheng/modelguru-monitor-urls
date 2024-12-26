import os
import aiohttp
import asyncio
import datetime
import json
from dotenv import load_dotenv

load_dotenv()

# Global configuration
filters = ['30_days', '7_days', '1_day', '1_year', '6_months', '3_months']
print(f"Script started by {os.getenv('GITHUB_ACTOR', 'local user')} at {datetime.datetime.utcnow().isoformat()}")

def get_time_range(filter_option):
    """
    Calculate start and end timestamps based on filter option.
    Returns timestamps in YYYYMMDDHHMMSS format.
    """
    now = datetime.datetime.utcnow()
    
    time_ranges = {
        '7_days': 7,
        '1_day': 1,
        '30_days': 30,
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
        'DOMAIN': os.getenv('DOMAIN'),
        'CLOUDFLARE_API_TOKEN': os.getenv('CLOUDFLARE_API_TOKEN'),
        'CLOUDFLARE_ACCOUNT_ID': os.getenv('CLOUDFLARE_ACCOUNT_ID'),
        'CLOUDFLARE_D1_DATABASE_ID': os.getenv('CLOUDFLARE_D1_DATABASE_ID'),
        'TIME_FRAME': os.getenv('TIME_FRAME', '0')
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

async def check_url_exists(platform, session, url, api_token, account_id, database_id):
    """Check if URL already exists in the table"""
    check_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "sql": f"SELECT COUNT(*) as count FROM wayback_{platform}_hashtag_data WHERE url = ?",
        "params": [url]
    }
    print('check existing sql',  f"SELECT COUNT(*) as count FROM wayback_{platform}_hashtag_data WHERE url = ?")

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

async def write_to_cloudflare_d1(platform, session, data, api_token, account_id, database_id):
    """Write data to Cloudflare D1"""
    url_exists = await check_url_exists(platform, session, data['url'], api_token, account_id, database_id)
    
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
        "sql": f"INSERT INTO wayback_{platform}_hashtag_data (url, date, updateAt) VALUES (?, ?, ?)",
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

async def get_urls_ccindex(platform, domain, api_token, account_id, database_id, timeframe):
    """Fetch URLs from Common Crawl Index and store them"""
    domainname = domain.replace("https://", "").split('/')[0]
    query_url = f"https://index.commoncrawl.org/CC-MAIN-2024-40-index?url={domainname}/*&output=json"
    
    try:
        timeframe_index = int(timeframe)
        if timeframe_index < 0 or timeframe_index >= len(filters):
            print(f"⚠ Invalid timeframe index {timeframe_index}, using default (2)")
            timeframe_index = 2
    except ValueError:
        print("⚠ Invalid timeframe value, using default (2)")
        timeframe_index = 2

    start, end = get_time_range(filters[timeframe_index])

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(query_url) as resp:
                if resp.status != 200:
                    print(f"✗ Common Crawl Index returned status {resp.status}")
                    return

                content = await resp.text()
                lines = content.splitlines()
                
                os.makedirs('./result', exist_ok=True)
                
                print(f"\nProcessing {len(lines)} URLs...")
                total_processed = 0
                total_skipped = 0

                for line in lines:
                    data = json.loads(line)
                    if 'url' in data:
                        url = data['url']
                        if domainname in url:
                            url = url.split(domainname)[-1]
                            if '&' in url:
                                url = url.split('&')[0]
                            data = {
                                "url": url,
                                "date": data['timestamp']
                            }
                            await write_to_cloudflare_d1(platform, session, data, api_token, account_id, database_id)

                print(f"\n✓ Processing complete:")
                print(f"  - Total URLs found: {len(lines)}")
                print(f"  - URLs processed: {total_processed}")
                print(f"  - URLs skipped (already exist): {total_skipped}")

        except Exception as e:
            print(f"✗ Error: {str(e)}")

async def create_table(platform, api_token, account_id, database_id):
    """Create the wayback_data table if it doesn't exist"""
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "sql": f"""
        CREATE TABLE IF NOT EXISTS wayback_{platform}_hashtag_data (
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

    domain = env_vars['DOMAIN'].lower()

    # Define the list of platforms with their URLs
    platforms = {
        "Facebook": "https://www.facebook.com/hashtag/",
        "Instagram": "https://www.instagram.com/explore/tags/",
        "Vkontakte": "https://vk.com/search?c%5Bq%5D=%23",
        "myMail": "https://my.mail.ru/hashtag/",
        "Pinterest": "https://www.pinterest.com/search/pins/?q=%23",
        "Tumblr": "https://www.tumblr.com/search/%23",
        "Twitter": "https://twitter.com/search?q=%23",
        "Telegram": "https://lyzem.com/search?q=%23",
        "Reddit": "https://www.reddit.com/search/?q=%23",
        "Clubhouse": "https://clubhousedb.com/search-clubs?q=%23",
        "Youtube": "https://www.youtube.com/hashtag/",
        "Twitch": "https://www.twitch.tv/search?term=%23",
        "Medium": "https://medium.com/search?q=%23",
        "Livejournal": "https://www.livejournal.com/rsearch?tags=",
        "Yandexzen": "https://zen.yandex.ru/search?query=%23",
        "Baidutieba": "https://tieba.baidu.com/f/search/res?qw=%23",
        "Weibo": "https://s.weibo.com/weibo?q=%23",
        "Yycom": "https://www.yy.com/search-#",
        "Myspace": "https://myspace.com/search?q=%23",
        "Skyrock": "https://www.skyrock.com/search/articles/?q=%23",
        "Thriller": "https://triller.co/search?search=%23",
        "Likee": "https://likee.video/search/%23",
        "Fark": "https://www.fark.com/hlsearch?qq=%23",
        "Devianart": "https://www.deviantart.com/search?q=%23",
        "Reverbnation": "https://www.reverbnation.com/main/search?q=%23",
        "Wattpad": "https://www.wattpad.com/search/%23",
        "Soundcloud": "https://soundcloud.com/search?q=%23",
        "Flickr": "https://www.flickr.com/search/?text=%23",
        "Digg": "https://digg.com/search?q=%23",
        "Hubpages": "https://discover.hubpages.com/search?query=%23",
        "Snapchat": "https://story.snapchat.com/search?q=",
        "Quora": "https://www.quora.com/search?q=%23",
        "Tiktok": "https://www.tiktok.com/tag/",
        "Vimeo": "https://vimeo.com/search?q=%23",
        "Douban": "https://www.douban.com/search?source=suggest&q=%23",
        "Douyin": "https://www.douyin.com/search/%23",
        "Kuaishou": "https://www.kuaishou.com/search/video?searchKey=%23",
        "Piscart": "https://picsart.com/search?q=%23",
        "Girlsaskguys": "https://www.girlsaskguys.com/search?q=%23",
        "Producthunt": "https://www.producthunt.com/search?q=%23",
        "Kikstarter": "https://www.kickstarter.com/discover/advanced?ref=nav_search&term=%23",
        "Fotki": "https://search.fotki.com/?q=%23",
        "Bilibili": "https://search.bilibili.com/all?keyword=%23",
        "Ixigua": "https://www.ixigua.com/search/%23",
        "Huya": "https://www.huya.com/search?hsk=%23",
        "Meipai": "https://www.meipai.com/search/all?q=%23",
        "Gofundme": "https://www.gofundme.com/s?q=%23",
        "Dribbble": "https://dribbble.com/search/#",
        "xhs": "https://www.xiaohongshu.com/search_result/?keyword=",
        "ideogram": "https://ideogram.ai/assets/progressive-image/balanced/response/",
        "crazygames": "https://crazygames.com/"
    }

    # Check if the domain is in the list of platforms
    if domain not in [platform.lower() for platform in platforms.keys()]:
        print(f"Domain {domain} not in the list of supported platforms")
        sys.exit(1)

    platform_url = platforms.get(domain.capitalize())

    await create_table(
        domain,
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID']
    )

    await get_urls_ccindex(
        domain,
        platform_url,
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID'],
        env_vars['TIME_FRAME']
    )

if __name__ == "__main__":
    asyncio.run(main())
