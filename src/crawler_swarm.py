import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Set, Optional
import aiohttp
import math

@dataclass
class RateLimitState:
    requests: int = 0
    window_start: datetime = datetime.now()
    backoff_until: Optional[datetime] = None
    consecutive_errors: int = 0

class CrawlerSwarm:
    def __init__(self, max_requests_per_minute: int = 60):
        self.max_rpm = max_requests_per_minute
        self.domain_states: Dict[str, RateLimitState] = {}
        self.visited_urls: Set[str] = set()
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def _get_domain(self, url: str) -> str:
        return url.split('/')[2]

    def _should_backoff(self, state: RateLimitState) -> bool:
        if not state.backoff_until:
            return False
        return datetime.now() < state.backoff_until

    async def _apply_rate_limit(self, domain: str):
        state = self.domain_states.get(domain, RateLimitState())
        self.domain_states[domain] = state

        now = datetime.now()
        window_elapsed = (now - state.window_start).total_seconds()

        if window_elapsed >= 60:
            state.requests = 0
            state.window_start = now
        
        if self._should_backoff(state):
            await asyncio.sleep((state.backoff_until - now).total_seconds())
            state.backoff_until = None

        if state.requests >= self.max_rpm:
            sleep_time = 60 - window_elapsed
            await asyncio.sleep(sleep_time)
            state.requests = 0
            state.window_start = datetime.now()

    def _calculate_backoff(self, state: RateLimitState) -> timedelta:
        base_delay = 5
        max_delay = 300  # 5 minutes
        delay = min(base_delay * (2 ** state.consecutive_errors), max_delay)
        return timedelta(seconds=delay)

    async def crawl_url(self, url: str) -> Optional[str]:
        if url in self.visited_urls:
            return None

        domain = self._get_domain(url)
        await self._apply_rate_limit(domain)

        state = self.domain_states[domain]
        
        try:
            async with self.session.get(url) as response:
                state.requests += 1
                self.visited_urls.add(url)

                if response.status == 429:  # Too Many Requests
                    state.consecutive_errors += 1
                    state.backoff_until = datetime.now() + self._calculate_backoff(state)
                    return None

                if response.status == 200:
                    state.consecutive_errors = 0
                    return await response.text()
                
                return None

        except Exception as e:
            state.consecutive_errors += 1
            state.backoff_until = datetime.now() + self._calculate_backoff(state)
            return None

    async def crawl_urls(self, urls: list[str]):
        tasks = [self.crawl_url(url) for url in urls]
        return await asyncio.gather(*tasks)
