import asyncio
from dataclasses import dataclass
from typing import Dict, Set, Optional
import aiohttp
import time
from collections import defaultdict

@dataclass
class RateLimiter:
    requests_per_second: float
    last_request_time: float = 0.0
    tokens: float = 0.0

    def update_tokens(self):
        now = time.time()
        time_passed = now - self.last_request_time
        self.tokens = min(self.requests_per_second, 
                         self.tokens + time_passed * self.requests_per_second)
        self.last_request_time = now

    async def acquire(self):
        while self.tokens < 1:
            self.update_tokens()
            await asyncio.sleep(0.1)
        self.tokens -= 1

class CrawlerSwarm:
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self.active_crawlers: Set[str] = set()
        self.domain_limiters: Dict[str, RateLimiter] = defaultdict(
            lambda: RateLimiter(requests_per_second=2.0)
        )
        self.backoff_times: Dict[str, float] = {}
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def get_backoff_time(self, domain: str) -> float:
        if domain not in self.backoff_times:
            return 1.0
        return min(300, self.backoff_times[domain] * 2)

    async def crawl_url(self, url: str) -> dict:
        from urllib.parse import urlparse
        domain = urlparse(url).netloc

        while len(self.active_crawlers) >= self.max_concurrent:
            await asyncio.sleep(0.1)

        await self.domain_limiters[domain].acquire()

        try:
            self.active_crawlers.add(url)
            if domain in self.backoff_times:
                await asyncio.sleep(self.backoff_times[domain])

            async with self.session.get(url) as response:
                if response.status == 429:  # Too Many Requests
                    self.backoff_times[domain] = self.get_backoff_time(domain)
                    return {"success": False, "error": "Rate limited"}
                
                if response.status == 200:
                    self.backoff_times.pop(domain, None)
                    content = await response.text()
                    return {"success": True, "content": content}
                
                return {"success": False, "error": f"Status {response.status}"}

        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self.active_crawlers.remove(url)

    async def crawl_batch(self, urls: list[str]) -> list[dict]:
        tasks = [self.crawl_url(url) for url in urls]
        return await asyncio.gather(*tasks)

    def get_swarm_stats(self) -> dict:
        return {
            "active_crawlers": len(self.active_crawlers),
            "domain_limiters": {
                domain: {"rps": limiter.requests_per_second,
                        "tokens": limiter.tokens}
                for domain, limiter in self.domain_limiters.items()
            },
            "backoff_times": self.backoff_times
        }
