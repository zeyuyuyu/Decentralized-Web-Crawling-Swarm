import asyncio
from typing import List, Set, Dict
from urllib.parse import urlparse
import aiohttp
from collections import defaultdict
import time

class CrawlerSwarm:
    def __init__(self, max_workers: int = 10, requests_per_second: int = 5):
        self.max_workers = max_workers
        self.requests_per_second = requests_per_second
        self.visited_urls: Set[str] = set()
        self.url_queue: asyncio.Queue = asyncio.Queue()
        self.domain_queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.domain_last_request: Dict[str, float] = defaultdict(float)
        self.session: aiohttp.ClientSession = None
    
    async def initialize(self):
        self.session = aiohttp.ClientSession()
    
    async def close(self):
        if self.session:
            await self.session.close()
    
    def get_domain(self, url: str) -> str:
        return urlparse(url).netloc
    
    async def rate_limit(self, domain: str):
        current_time = time.time()
        time_since_last = current_time - self.domain_last_request[domain]
        if time_since_last < (1.0 / self.requests_per_second):
            await asyncio.sleep((1.0 / self.requests_per_second) - time_since_last)
        self.domain_last_request[domain] = time.time()
    
    async def crawl_url(self, url: str) -> List[str]:
        if url in self.visited_urls:
            return []
        
        domain = self.get_domain(url)
        await self.rate_limit(domain)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    self.visited_urls.add(url)
                    text = await response.text()
                    # Basic link extraction - could be enhanced
                    found_urls = []
                    # Process page content here
                    return found_urls
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            return []
    
    async def worker(self):
        while True:
            url = await self.url_queue.get()
            domain = self.get_domain(url)
            await self.domain_queues[domain].put(url)
            
            new_urls = await self.crawl_url(url)
            for new_url in new_urls:
                if new_url not in self.visited_urls:
                    await self.url_queue.put(new_url)
            
            self.url_queue.task_done()
    
    async def crawl(self, start_urls: List[str]):
        await self.initialize()
        
        for url in start_urls:
            await self.url_queue.put(url)
        
        workers = [asyncio.create_task(self.worker()) 
                  for _ in range(self.max_workers)]
        
        await self.url_queue.join()
        
        for worker in workers:
            worker.cancel()
        
        await self.close()
        return list(self.visited_urls)
