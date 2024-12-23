import aiohttp
import json,os
from DataRecorder import Recorder
import csv
import asyncio
from dotenv import load_dotenv
load_dotenv()
import datetime

proxy_url=None
domain='toolify.ai'
domain='claude.site'
domain='revved.com'
domain='https://www.amazon.com/sp?ie=UTF8&seller='
domain='https://www.amazon.com/sp?'
domain='https://www.amazon.com/sp?_encoding=UTF8&marketplaceID=ATVPDKIKX0DER&orderID=&seller='
domain='https://www.amazon.com/sp?_encoding=UTF8&marketplaceID='
domain='https://www.amazon.com/sp'
domain='https://tophub.today'
# domain='https://www.amazon.com/s'
# domain='https://www.amazon.com/sp?ie=UTF8&seller='
domain = os.getenv('domain')
if domain is None:
    domain='https://www.amazon.com/sp'

os.makedirs('./result', exist_ok=True)

async def geturls(domain):
    no_subs=None
    subs_wildcard = "*." if not no_subs else ""
    # subs_wildcard=''
    domainname = domain.replace("https://", "")
    domainname=domainname.split('/')[0]
    csv_file=f'waybackmachines-{domainname}.csv'
    date_today = datetime.date.today().strftime("%Y-%m-%d")

    outfile=Recorder(f'result/{domainname}-{date_today}.csv')

    query_url = f"http://web.archive.org/cdx/search/cdx?url={subs_wildcard}{domain}/&fl=timestamp,original,mimetype,statuscode,digest"
    query_url = f"http://web.archive.org/cdx/search/cdx?url={domain}/&matchType=prefix&fl=timestamp,original"
    fileter='&statuscode=200'
    filter="&collapse=digest"
    filter="&collapse=urlkey"
    query_url=query_url+filter

    # For example: http://web.archive.org/cdx/search/cdx?url=archive.org&from=2010&to=2011



    # query_url='https://web.archive.org/__wb/sparkline?output=json&url=toolify.ai&collection=web'
    headers = {'Referer': 'https://web.archive.org/',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                'Chrome/92.0.4515.107 Safari/537.36'}
    # initialize an AIOHTTP client with the SOCKS proxy connector
    async with aiohttp.ClientSession(connector=None) as session:
        # Perform your web requests using the session
        try:
            resp=await session.get(query_url,
                                headers=headers,
                # auth=auth.prepare_request, 
                timeout=300000)     
            count=0
            while True:
                raw = await resp.content.read(10240)  # 每次读取1024字节
                if not raw:
                    break
                # raw=await resp.text()
                # print(raw)
                try:

                    raw = raw.decode('utf-8')
                except UnicodeDecodeError:
                    raw = raw.decode('latin-1')

                if resp.status != 200:
                # Handle different HTTP status codes
                # return 
                    print('not 200')
            # Process the response
                # print(raw)
                lines = raw.splitlines()
                fieldnames = ['date', 'url']

                print(len(lines))
                count=count+len(lines)
                file_exists = os.path.isfile(csv_file)

                for line in lines:
                    if ' ' in line:
                        # parts = line.split(' ')
                        # timestamp, original_url = parts[0], parts[1]
                        # data={'date':timestamp,'url':original_url}
                        data={'url':line.strip()}
                        with open(csv_file, mode='a', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=fieldnames)
                            # if not file_exists:
                                # writer.writeheader()
                            writer.writerow(data)
            print('============',count)

        except aiohttp.ClientError as e:
            print(f"Connection error: {e}", 'red')
        except Exception as e:
            print(f"Couldn't get list of responses: {e}", 'red')
            # out.append(Wurl(urls[1], urls[2]))
        # return out
        outfile.record()
asyncio.run(geturls(domain))
