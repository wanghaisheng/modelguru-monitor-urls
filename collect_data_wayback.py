import requests as rq
import time
import os
import argparse
import sys
from tqdm import tqdm

sys.path.insert(1, os.path.join(sys.path[0], '..'))

def collect_data_wayback(website_url,
                         output_dir,
                         start_date,
                         end_date,
                         resume_key='',
                         max_count=1000,
                         chunk_size=100,
                         sleep=3,
                         retries=5):
    if 'http://' in website_url:
        website_url = website_url.replace('http://', '')
    if 'https://' in website_url:
        website_url = website_url.replace('https://', '')

    if chunk_size > max_count:
        raise ValueError('Chunk size needs to be smaller than max count.')

    unique_articles_set = set()
    url_list = []
    url_template = 'http://web.archive.org/cdx/search/cdx?url=https://www.{domain}&collapse=urlkey&filter=!statuscode:404&showResumeKey=true&matchType=prefix&from={start}&to={end}&limit={chunk}&output=json'
    
    url = url_template.format(domain=website_url, start=start_date, end=end_date, chunk=chunk_size)
    if resume_key:
        url += '&resumeKey=' + resume_key

    its = max_count // chunk_size
    progress_bar = tqdm(total=its)

    for _ in range(its):
        for attempt in range(retries):
            try:
                result = rq.get(url)
                result.raise_for_status()
                parse_url = result.json()

                if len(parse_url) < 2:
                    print("No more data to fetch.")
                    progress_bar.close()
                    return url_list
                
                # Update resume_key and URL for the next batch
                new_resume_key = parse_url[-1][0] if parse_url[-1][0] != resume_key else ''
                if not new_resume_key:
                    print("No progress detected with resume key. Exiting loop.")
                    progress_bar.close()
                    return url_list
                
                resume_key = new_resume_key
                for i in range(1, len(parse_url) - 1):
                    # print('====',parse_url[i])
                    if len(parse_url[i])<5:
                      continue
                    orig_url = parse_url[i][2]
                    print('======',orig_url)

                    if parse_url[i][4] != '200':
                        continue
                    if orig_url not in unique_articles_set:
                        url_list.append(orig_url)
                        unique_articles_set.add(orig_url)
                
                url = url_template.format(domain=website_url, start=start_date, end=end_date, chunk=chunk_size) + '&resumeKey=' + resume_key
                time.sleep(sleep)
                break
            except (rq.RequestException, ValueError) as e:
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    print(f"Failed to fetch data after {retries} attempts. Error: {e}")
                    progress_bar.close()
                    return url_list
        print('current url',len(url_list))

        progress_bar.update(1)

        # Check if the progress is complete
        if progress_bar.n == its:
            print("Progress completed. Returning results.")
            progress_bar.close()
            return url_list

    progress_bar.close()
    print('urls count', len(url_list))
    print('Collected %s of the initial number of requested urls' % (round(len(url_list) / max_count, 2)))
    return url_list


import random

# Load SOCKS5 proxies from a URL (or you can load from a file if you prefer)
def load_proxies(url):
    if url is None:
      url='https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt'
      # https://github.com/proxifly/free-proxy-list/blob/main/proxies/all/data.txt
    try:
        response = rq.get(url)
        response.raise_for_status()
        return response.text.splitlines()
    except rq.RequestException as e:
        print(f"Error fetching proxy list from {url}: {e}")
        return []

# Function to select a random proxy
def get_random_proxy(proxy_list):
    return random.choice(proxy_list) if proxy_list else None

sys.path.insert(1, os.path.join(sys.path[0], '..'))

def exact_url_timestamp(website_url,
                         sleep=3,
                         retries=5,
                         max_count=1000,
                         chunk_size=100,
                        
                         proxy_retries=3,  # Added retry limit for proxies
                         proxies=None):  # Added proxies parameter
    if 'http://' in website_url:
        website_url = website_url.replace('http://', '')
    if 'https://' in website_url:
        website_url = website_url.replace('https://', '')


    unique_articles_set = set()
    items = []
    url = f'http://web.archive.org/cdx/search/cdx?url=https://www.{website_url}&collapse=urlkey&filter=!statuscode:404&showResumeKey=true&matchType=exact&limit=1&output=json'    
    # max_count=1
    # chunk_size=1
    its = max_count // chunk_size
    progress_bar = tqdm(total=its)

    for _ in range(its):
        for attempt in range(retries):
            for proxy_attempt in range(proxy_retries):  # Retry with different proxies if request fails
                try:
                    # Select a random proxy from the list
                    if proxies is None:
                        proxies=load_proxies()
                    proxy = get_random_proxy(proxies)
                    proxy_dict = None
                    if 'socks5' in proxy==False:
                        # Define the proxy for requests
                        proxy_dict = {'http': f'socks5://{proxy}', 'https': f'socks5://{proxy}'}
                    else:
                        proxy_dict = {'http': f'{proxy}', 'https': f'{proxy}'}
                      
                    result = rq.get(url, proxies=proxy_dict)
                    result.raise_for_status()
                    parse_url = result.json()

                    if len(parse_url) < 2:
                        print("No more data to fetch.")
                        progress_bar.close()
                        return url_list
                                        
                    for i in range(1, len(parse_url) - 1):
                        if len(parse_url[i]) < 5:
                            continue
                        orig_url = parse_url[i][2]
                        indexdate = parse_url[i][1]
                        item={}
                        item['url']=orig_url
                        item['timestamp']=indexdate
                        items.append(item)

                    print('======', items)
                      
                    break  # Exit proxy retry loop if successful
                except (rq.RequestException, ValueError) as e:
                    if proxy_attempt < proxy_retries - 1:
                        print(f"Proxy failed. Retrying with another proxy. Attempt {proxy_attempt + 1}/{proxy_retries}")
                        continue  # Try another proxy
                    else:
                        print(f"Failed to fetch data after {proxy_retries} proxy attempts. Error: {e}")
                        progress_bar.close()
                        return items
            print('current url index date', items)

            progress_bar.update(1)

            # Check if the progress is complete
            if progress_bar.n == its:
                print("Progress completed. Returning results.")
                progress_bar.close()
                return items

    progress_bar.close()
    print('urls count', len(items))
    print('Collected %s of the initial number of requested urls' % (round(len(items) / max_count, 2)))
    return items



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download articles and images from the Wayback machine.')
    parser.add_argument('--url_domain', type=str, default='factly.in/', 
                        help='The domain to query on the Wayback Machine API. factly.in/, 211check.org/, or pesacheck.org/')
    parser.add_argument('--file_path', type=str, default='dataset/url/factly.txt',
                        help='Path to the file that stores the URLs')
    parser.add_argument('--start_date', type=int, default=20190101,
                        help='Start date for the collection of URLs.')
    parser.add_argument('--end_date', type=int, default=20231231,
                        help='End date for the collection of URLs.')
    parser.add_argument('--max_count', type=int, default=20000,
                        help='Maximum number of URLs to collect.')
    parser.add_argument('--chunk_size', type=int, default=4000,
                        help='Size of each chunk to query the Wayback Machine API.')
    parser.add_argument('--sleep', type=int, default=5,
                        help='Waiting time between two calls of the Wayback machine API.')

    args = parser.parse_args()

    collect_data_wayback(args.url_domain,
                         args.file_path,
                         start_date=args.start_date,
                         end_date=args.end_date,
                         max_count=args.max_count,
                         chunk_size=args.chunk_size,
                         sleep=args.sleep)
