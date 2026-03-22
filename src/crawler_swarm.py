import random
import time
import asyncio
from typing import List, Tuple

class CrawlerNode:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.task_queue = asyncio.Queue()
        self.load = 0

    async def run(self):
        while True:
            task = await self.task_queue.get()
            await self.process_task(task)
            self.task_queue.task_done()
            self.load -= 1

    async def process_task(self, task):
        # Implement task processing logic here
        await asyncio.sleep(random.uniform(0.5, 2.0))

class CrawlerSwarm:
    def __init__(self, num_nodes: int):
        self.nodes: List[CrawlerNode] = [CrawlerNode(f'node_{i}') for i in range(num_nodes)]
        self.tasks: List[Tuple[str, str]] = []

    async def add_task(self, url: str, domain: str):
        self.tasks.append((url, domain))
        await self.allocate_task()

    async def allocate_task(self):
        least_loaded_node = min(self.nodes, key=lambda node: node.load)
        task = self.tasks.pop(0)
        await least_loaded_node.task_queue.put(task)
        least_loaded_node.load += 1

    async def run(self):
        tasks = [node.run() for node in self.nodes]
        await asyncio.gather(*tasks)

if __name__ == '__main__':
    swarm = CrawlerSwarm(num_nodes=10)

    async def main():
        await swarm.add_task('https://www.example.com', 'example.com')
        await swarm.add_task('https://www.google.com', 'google.com')
        await swarm.add_task('https://www.reddit.com', 'reddit.com')
        await swarm.run()

    asyncio.run(main())
