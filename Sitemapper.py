import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import urllib.parse

class Sitemapper:

    def __init__(self):
        self.urls_crawled = set()
        self.urls_queue = set()
        self.max_urls = 100

    async def main(self, start_url, block_extensions=['.pdf'], max_urls=100):
        self.max_urls = max_urls
        scheme, netloc, path, params, query, fragment = urllib.parse.urlparse(start_url)
        fragments = (scheme, netloc, '', '', '', '')
        base_url = urllib.parse.urlunparse(fragments)

        self.urls_queue.add(base_url)

        async with aiohttp.ClientSession() as session:
            tasks = []
            while self.urls_queue and len(self.urls_crawled) < self.max_urls:
                url = self.urls_queue.pop()
                tasks.append(self.fetch(session, url, block_extensions))
                if len(tasks) >= 10:  # Control concurrency level
                    await asyncio.gather(*tasks)
                    tasks = []

            if tasks:
                await asyncio.gather(*tasks)

        return self.urls_crawled

    async def fetch(self, session: ClientSession, url: str, block_extensions: list):
        print(f"Fetching: {url}")
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    body = await response.text()
                    self.urls_crawled.add(url)
                    links = self.extract_links(url, body, block_extensions)
                    for link in links:
                        if link not in self.urls_queue and link not in self.urls_crawled:
                            self.urls_queue.add(link)
        except aiohttp.ClientError as e:
            print(f"Error fetching {url}: {e}")

    def extract_links(self, url, body, block_extensions):
        soup = BeautifulSoup(body, 'html.parser')
        links = soup.find_all('a', href=True)

        good_links = []
        for link in links:
            partial_link_url = link['href']
            link_url = urllib.parse.urljoin(url, partial_link_url)

            scheme, netloc, path, _, _, _ = urllib.parse.urlparse(link_url)
            base_scheme, base_netloc, _, _, _, _ = urllib.parse.urlparse(url)

            if any(path.endswith(ext) for ext in block_extensions):
                continue

            if netloc == base_netloc:
                good_links.append(link_url)

        return good_links

(*     async def find_model_urls(self, start_url, keyword="model", block_extensions=['.pdf'], max_urls=100): *)
(*         crawled_urls = await self.main(start_url, block_extensions, max_urls) *)
(*         model_urls = [url for url in crawled_urls if keyword in url.lower()] *)
(*         return model_urls *)

# Example usage:
# asyncio.run(sitemapper.find_model_urls("https://example.com", keyword="model"))
