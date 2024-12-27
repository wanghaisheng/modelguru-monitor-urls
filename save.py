from urllib.parse import urlparse, parse_qs, unquote

import aiohttp
import csv
import os
import asyncio
import datetime
from dotenv import load_dotenv
import sys
from waybackpy import WaybackMachineCDXServerAPI
from waybackpy.wrapper import Url
import cdx_toolkit

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

    start_timestamp = start.strftime("%Y%m%d")
    end_timestamp = end.strftime("%Y%m%d")

    print(f"Date range: {start.isoformat()} to {end.isoformat()} UTC")
    return start_timestamp, end_timestamp

def check_environment_variables():
    """Check and validate required environment variables"""
    required_vars = {
        'DOMAIN': os.getenv('DOMAIN', 'reddit'),
        'CLOUDFLARE_API_TOKEN': os.getenv('CLOUDFLARE_API_TOKEN'),
        'CLOUDFLARE_ACCOUNT_ID': os.getenv('CLOUDFLARE_ACCOUNT_ID'),
        'CLOUDFLARE_D1_DATABASE_ID': os.getenv('CLOUDFLARE_D1_DATABASE_ID'),
        'TIME_FRAME': os.getenv('time_frame', '0')
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

async def check_tag_exists(platform,session, tag, api_token, account_id, database_id):
    """Check if URL already exists in the table"""
    check_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    payload = {
        "sql": f"SELECT COUNT(*) as count FROM wayback_{platform}_hashtag_data WHERE tag = ?",
        "params": [tag]
    }
    print('check existing sql',  f"SELECT COUNT(*) as count FROM wayback_{platform}_hashtag_data WHERE tag = ?"
)
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

async def write_to_cloudflare_d1(platform,session, data, api_token, account_id, database_id):
    """Write data to Cloudflare D1"""
    tag_exists = await check_tag_exists(platform,session, data['tag'], api_token, account_id, database_id)
    
    if tag_exists:
        print(f"⚠ URL already exists, skipping: {data['tag']}")
        return

    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_token}"
    }
    
    current_time = datetime.datetime.utcnow().isoformat()
    
    payload = {
        "sql": f"INSERT INTO wayback_{platform}_hashtag_data (tag,url, date, updateAt) VALUES (?,?, ?, ?)",
        "params": [data['tag'],data['url'], data['date'], current_time]
    }

    try:
        async with session.post(url, headers=headers, json=payload) as response:
            if response.status == 200:
                print(f"✓ Successfully inserted: {data['tag'],data['url']}")
            else:
                error_text = await response.text()
                print(f"✗ Failed to insert: {data['tag'],data['url']}")
                print(f"Error: {error_text}")
    except Exception as e:
        print(f"✗ Error writing to Cloudflare: {str(e)}")
import re

# Function to replace emojis in a string
def replace_emojis(text, replacement=""):
    # Regex pattern to match emojis
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
        "\U0001F680-\U0001F6FF"  # Transport & Map Symbols
        "\U0001F700-\U0001F77F"  # Alchemical Symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"  # Enclosed Characters
        "\U0001F1E6-\U0001F1FF"  # Flags (iOS)
        "]+",
        flags=re.UNICODE,
    )
    # Replace all emojis with the replacement text
    return emoji_pattern.sub(replacement, text)




async def saveurls(platform, domain, api_token, account_id, database_id, timeframe):
    """
    Fetch URLs from Wayback Machine using waybackpy and store them in the Cloudflare D1 database.
    """
    domainname = domain.replace("https://", "").split('/')[0]
    print(f"\nFetching URLs for domain: {domainname}")

    try:
        timeframe_index = int(timeframe)
        if timeframe_index < 0 or timeframe_index >= len(filters):
            print(f"⚠ Invalid timeframe index {timeframe_index}, using default (2)")
            timeframe_index = 2
    except ValueError:
        print("⚠ Invalid timeframe value, using default (2)")
        timeframe_index = 2
    website_url=domain.replace('https://','')
    website_url=website_url.replace('www.','')    

    start, end = get_time_range(filters[timeframe_index])

    print(f"Timeframe: {filters[timeframe_index]}")
    print(f"Start: {start}, End: {end}")

    try:
        if not os.path.exists('result.txt'):
            return 
        lines=[]
        with open('result.txt',encoding='utf8') as f:
            lines=f.readlines()
        
        
        async with aiohttp.ClientSession() as session:
            # for snapshot in urls:
            for line in lines:
                obj=line.split(',')
                print('=======',obj)
                if len(obj)==0:
                    return 
                obj=[x.strip() for x in obj]
                url=obj[2].replace('url','').strip().replace('\n','')
                date=obj[1].replace('timestamp','').strip()
                print('url',url)
                print('date',date)
                print('website',website_url)

                parsed_url = urlparse(url)
                path = parsed_url.path
                decoded_path = unquote(path)
                tag=None
                if website_url=='tiktok.com':
                    tag=decoded_path.split('tag')[0]
                    if 'pc' in tag:
                        tag=tag.split('/pc')[0]
            
                tag = replace_emojis(tag, replacement="")
                
                print('keep params clean',tag)
                    
                
                data = {
                    # "url": snapshot.archive_url,
                    # "date": snapshot.timestamp
                }
                data={
                "tag":tag,
                "url":url,
                'date':date
                }
                await write_to_cloudflare_d1(platform, session, data, api_token, account_id, database_id)

        print(f"\n✓ Completed fetching and storing URLs for domain: {domainname}")

    except Exception as e:
        print(f"✗ Error using waybackpy: {str(e)}")

async def geturls(platform,domain, api_token, account_id, database_id, timeframe):
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
    print('start,end',start,end)
    chunk_size=4000
    website_url=domain.replace('https://','')
    website_url=website_url.replace('www.','')    

    headers = {
        'Referer': 'https://web.archive.org/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    query_url = 'http://web.archive.org/cdx/search/cdx?url=https://www.' + website_url + '*&collapse=urlkey&filter=!statuscode:404&showResumeKey=true&matchType=prefix&from=%s&to=%s&limit=%s&output=json'%(start,end,chunk_size)
# https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md


    query_url='http://web.archive.org/cdx/search/cdx?url=tiktok.com/tag/&collapse=urlkey&matchType=prefix&from=20241223&to=20241225'
    query_url='http://web.archive.org/cdx/search/cdx?url=tiktok.com/tag/&collapse=digest&matchType=prefix&from=2024&to=2024&fl=original,timestamp'
    query_url='http://web.archive.org/cdx/search/cdx?url=tiktok.com/tag/&collapse=urlkey&matchType=prefix&from=2023&to=2023'
    query_url='http://web.archive.org/cdx/search/cdx?url=tiktok.com/tag/&collapse=urlkey&matchType=prefix&from=2024&to=2024'
    query_url=query_url+'&fl=timestamp,original'
    print('build query url',query_url,website_url)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(query_url, headers=headers,
                                   timeout=300000) as resp:
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
                        print('preprocessing',parts)
                        
                        if len(parts) >= 2:
                            url=parts[1]
                            print('preprocessing url',website_url in url, '?' in url,'&' in url)
                            
                            if website_url in url:
                                url=url.split(website_url)[-1]
                                print('keep params only',url)
                            if '?' in url:
            
                                url=url.split('?')[0]
                                
                            if '&' in url:
                                url=url.split('&')[0]
                                print('keep params clean',url)
                                
                            data = {
                                "url": url,
                                "date": parts[0]
                            }
                            await write_to_cloudflare_d1(platform,session, data, api_token, account_id, database_id)
                            
                            # url_exists = await check_tag_exists(platform,session, data['url'], api_token, account_id, database_id)
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

async def create_table(platform,api_token, account_id, database_id):
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
            tag TEXT NOT NULL UNIQUE,
            url TEXT NOT NULL,
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



# hashtag = "exampleHashtag"  # Replace with your actual hashtag

# Define the list of links for the given hashtag
    links = [
    {"Facebook": f"https://www.facebook.com/hashtag/"},
    {"Instagram": f"https://www.instagram.com/explore/tags/"},
    {"Vkontakte": f"https://vk.com/search?c%5Bq%5D=%23"},
    {"myMail": f"https://my.mail.ru/hashtag/"},
    {"Pinterest": f"https://www.pinterest.com/search/pins/?q=%23"},
    {"Tumblr": f"https://www.tumblr.com/search/%23"},
    {"Twitter": f"https://twitter.com/search?q=%23"},
    {"Telegram": f"https://lyzem.com/search?q=%23"},
    {"Reddit": f"https://www.reddit.com/search/?q=%23"},
    {"Clubhouse": f"https://clubhousedb.com/search-clubs?q=%23"},
    {"Youtube": f"https://www.youtube.com/hashtag/"},
    {"Twitch": f"https://www.twitch.tv/search?term=%23"},
    {"Medium": f"https://medium.com/search?q=%23"},
    {"Livejournal": f"https://www.livejournal.com/rsearch?tags="},
    {"Yandexzen": f"https://zen.yandex.ru/search?query=%23"},
    {"Baidutieba": f"https://tieba.baidu.com/f/search/res?qw=%23"},
    {"Weibo": f"https://s.weibo.com/weibo?q=%23"},
    {"Yycom": f"https://www.yy.com/search-#"},
    {"Myspace": f"https://myspace.com/search?q=%23"},
    {"Skyrock": f"https://www.skyrock.com/search/articles/?q=%23"},
    {"Thriller": f"https://triller.co/search?search=%23"},
    {"Likee": f"https://likee.video/search/%23"},
    {"Fark": f"https://www.fark.com/hlsearch?qq=%23"},
    {"Devianart": f"https://www.deviantart.com/search?q=%23"},
    {"Reverbnation": f"https://www.reverbnation.com/main/search?q=%23"},
    {"Wattpad": f"https://www.wattpad.com/search/%23"},
    {"Soundcloud": f"https://soundcloud.com/search?q=%23"},
    {"Flickr": f"https://www.flickr.com/search/?text=%23"},
    {"Digg": f"https://digg.com/search?q=%23"},
    {"Hubpages": f"https://discover.hubpages.com/search?query=%23"},
    {"Snapchat": f"https://story.snapchat.com/search?q="},
    {"Quora": f"https://www.quora.com/search?q=%23"},
    {"Tiktok": f"https://www.tiktok.com/tag/"},
    {"Vimeo": f"https://vimeo.com/search?q=%23"},
    {"Douban": f"https://www.douban.com/search?source=suggest&q=%23"},
    {"Douyin": f"https://www.douyin.com/search/%23"},
    {"Kuaishou": f"https://www.kuaishou.com/search/video?searchKey=%23"},
    {"Piscart": f"https://picsart.com/search?q=%23"},
    {"Girlsaskguys": f"https://www.girlsaskguys.com/search?q=%23"},
    {"Producthunt": f"https://www.producthunt.com/search?q=%23"},
    {"Kikstarter": f"https://www.kickstarter.com/discover/advanced?ref=nav_search&term=%23"},
    {"Fotki": f"https://search.fotki.com/?q=%23"},
    {"Bilibili": f"https://search.bilibili.com/all?keyword=%23"},
    {"Ixigua": f"https://www.ixigua.com/search/%23"},
    {"Huya": f"https://www.huya.com/search?hsk=%23"},
    {"Meipai": f"https://www.meipai.com/search/all?q=%23"},
    {"Gofundme": f"https://www.gofundme.com/s?q=%23"},
    {"Dribbble": f"https://dribbble.com/search/#"},
  {"xhs":"https://www.xiaohongshu.com/search_result/?keyword="},
        {'ideogram':"https://ideogram.ai/assets/progressive-image/balanced/response/"},
        {'crazygames':"https://crazygames.com/"}

        # tumblr
        # https://mashable.com/article/very-demure-very-mindful-meaning-tiktok
        # https://www.cosmopolitan.com/entertainment/celebs/a61865984/very-demure-tiktok-trend/
        # Add other links as needed
]
    domain=env_vars['DOMAIN'].lower()

# Print all links
    for link in links:
        for platform, url in link.items():
            platform=platform.lower()
            
            print('domain you input is',domain)
            print(f"current {platform}: {url}")
            
            if platform!=domain:
                continue
            
            await create_table(platform,
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID']
    )


            await saveurls(
        # env_vars['DOMAIN'],
          platform,
          url,
        env_vars['CLOUDFLARE_API_TOKEN'],
        env_vars['CLOUDFLARE_ACCOUNT_ID'],
        env_vars['CLOUDFLARE_D1_DATABASE_ID'],
        env_vars['TIME_FRAME']
    )

if __name__ == "__main__":
    asyncio.run(main())
