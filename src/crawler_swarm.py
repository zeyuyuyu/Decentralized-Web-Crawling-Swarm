import redis
import hashlib
import json
import time
from typing import List, Dict, Optional

class CrawlerSwarm:
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        """Initialize a crawler node in the distributed swarm."""
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.node_id = self._generate_node_id()
        self.active = False

    def _generate_node_id(self) -> str:
        """Generate a unique ID for this crawler node."""
        timestamp = str(time.time()).encode('utf-8')
        return hashlib.sha256(timestamp).hexdigest()[:12]

    def join_swarm(self):
        """Register this crawler node with the swarm."""
        self.active = True
        self.redis_client.sadd('active_crawlers', self.node_id)
        self.redis_client.hset(f'crawler:{self.node_id}', mapping={
            'last_heartbeat': time.time(),
            'urls_processed': 0,
            'status': 'idle'
        })

    def leave_swarm(self):
        """Gracefully remove this node from the swarm."""
        self.active = False
        self.redis_client.srem('active_crawlers', self.node_id)
        self.redis_client.delete(f'crawler:{self.node_id}')

    def heartbeat(self):
        """Update node's last active timestamp."""
        if self.active:
            self.redis_client.hset(f'crawler:{self.node_id}', 'last_heartbeat', time.time())

    def claim_urls(self, batch_size: int = 10) -> List[str]:
        """Claim a batch of URLs for processing."""
        pipeline = self.redis_client.pipeline()
        urls = []
        
        # Atomic claim operation
        for _ in range(batch_size):
            url = self.redis_client.rpoplpush('pending_urls', f'processing:{self.node_id}')
            if url:
                urls.append(url)

        return urls

    def mark_url_complete(self, url: str, metadata: Dict):
        """Mark a URL as processed and store its metadata."""
        pipeline = self.redis_client.pipeline()
        pipeline.lrem(f'processing:{self.node_id}', 1, url)
        pipeline.hset(f'completed:{url}', mapping=metadata)
        pipeline.hincrby(f'crawler:{self.node_id}', 'urls_processed', 1)
        pipeline.execute()

    def get_swarm_status(self) -> Dict:
        """Get status of all nodes in the swarm."""
        status = {
            'active_nodes': self.redis_client.scard('active_crawlers'),
            'pending_urls': self.redis_client.llen('pending_urls'),
            'nodes': {}
        }

        for node_id in self.redis_client.smembers('active_crawlers'):
            node_info = self.redis_client.hgetall(f'crawler:{node_id}')
            status['nodes'][node_id] = node_info

        return status

    def cleanup_dead_nodes(self, timeout: int = 300):
        """Remove nodes that haven't sent heartbeats."""
        current_time = time.time()
        for node_id in self.redis_client.smembers('active_crawlers'):
            last_heartbeat = float(self.redis_client.hget(f'crawler:{node_id}', 'last_heartbeat') or 0)
            if current_time - last_heartbeat > timeout:
                # Recover URLs from dead node
                urls = self.redis_client.lrange(f'processing:{node_id}', 0, -1)
                pipeline = self.redis_client.pipeline()
                for url in urls:
                    pipeline.lpush('pending_urls', url)
                pipeline.delete(f'processing:{node_id}')
                pipeline.srem('active_crawlers', node_id)
                pipeline.delete(f'crawler:{node_id}')
                pipeline.execute()
