import asyncio
from typing import List, Dict, Set
import aiohttp
import logging
from dataclasses import dataclass
from datetime import datetime
import random

@dataclass
class CrawlerNode:
    id: str
    last_heartbeat: datetime
    urls_processing: Set[str]
    urls_completed: Set[str]
    failure_count: int

class CrawlerSwarm:
    def __init__(self, max_nodes: int = 10):
        self.nodes: Dict[str, CrawlerNode] = {}
        self.max_nodes = max_nodes
        self.url_queue: asyncio.Queue = asyncio.Queue()
        self.results: Dict[str, dict] = {}
        self.node_timeout = 30  # seconds
        self.logger = logging.getLogger(__name__)

    async def register_node(self, node_id: str) -> bool:
        if len(self.nodes) >= self.max_nodes:
            return False
            
        self.nodes[node_id] = CrawlerNode(
            id=node_id,
            last_heartbeat=datetime.now(),
            urls_processing=set(),
            urls_completed=set(),
            failure_count=0
        )
        self.logger.info(f'Node {node_id} registered')
        return True

    async def heartbeat(self, node_id: str) -> None:
        if node_id in self.nodes:
            self.nodes[node_id].last_heartbeat = datetime.now()

    async def monitor_nodes(self):
        while True:
            now = datetime.now()
            dead_nodes = []
            
            for node_id, node in self.nodes.items():
                time_diff = (now - node.last_heartbeat).total_seconds()
                if time_diff > self.node_timeout:
                    dead_nodes.append(node_id)
                    self.logger.warning(f'Node {node_id} appears dead, redistributing work')
                    
            for node_id in dead_nodes:
                # Redistribute unfinished work
                dead_node = self.nodes[node_id]
                for url in dead_node.urls_processing:
                    await self.url_queue.put(url)
                del self.nodes[node_id]
                
            await asyncio.sleep(5)

    async def assign_work(self, node_id: str) -> List[str]:
        if node_id not in self.nodes:
            return []
            
        node = self.nodes[node_id]
        work_batch = []
        
        # Determine batch size based on node performance
        batch_size = 10 / (node.failure_count + 1)  # Reduce batch for unreliable nodes
        batch_size = max(1, min(10, int(batch_size)))
        
        try:
            for _ in range(batch_size):
                if self.url_queue.empty():
                    break
                url = await self.url_queue.get()
                work_batch.append(url)
                node.urls_processing.add(url)
        except Exception as e:
            self.logger.error(f'Error assigning work to node {node_id}: {str(e)}')
            
        return work_batch

    async def submit_results(self, node_id: str, results: Dict[str, dict]) -> None:
        if node_id not in self.nodes:
            return
            
        node = self.nodes[node_id]
        for url, result in results.items():
            if url in node.urls_processing:
                node.urls_processing.remove(url)
                node.urls_completed.add(url)
                self.results[url] = result
                
        # Update node reliability metrics
        success_rate = len(results) / max(1, len(node.urls_processing))
        if success_rate < 0.5:
            node.failure_count += 1
        else:
            node.failure_count = max(0, node.failure_count - 1)

    async def add_urls(self, urls: List[str]) -> None:
        for url in urls:
            if url not in self.results:
                await self.url_queue.put(url)

    def get_stats(self) -> dict:
        return {
            'active_nodes': len(self.nodes),
            'queued_urls': self.url_queue.qsize(),
            'completed_urls': len(self.results),
            'node_stats': [
                {
                    'id': n.id,
                    'urls_processing': len(n.urls_processing),
                    'urls_completed': len(n.urls_completed),
                    'failure_count': n.failure_count
                } for n in self.nodes.values()
            ]
        }
