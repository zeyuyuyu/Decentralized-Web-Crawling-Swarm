import random
import time
import requests
from multiprocessing import Process, Queue

class CrawlerNode:
    def __init__(self, node_id):
        self.node_id = node_id
        self.task_queue = Queue()
        self.health = 100

    def crawl(self):
        while True:
            if not self.task_queue.empty():
                url = self.task_queue.get()
                try:
                    response = requests.get(url)
                    print(f'Node {self.node_id} crawled {url} - Status code: {response.status_code}')
                    self.health += 10
                except:
                    self.health -= 20
                    print(f'Node {self.node_id} failed to crawl {url} - Health: {self.health}')
            else:
                time.sleep(1)

            if self.health <= 0:
                print(f'Node {self.node_id} has failed, restarting...')
                self.__init__(self.node_id)
                print(f'Node {self.node_id} restarted successfully')

class CrawlerSwarm:
    def __init__(self, num_nodes):
        self.num_nodes = num_nodes
        self.nodes = []
        self.load_balancer = LoadBalancer(self.nodes)

        for i in range(num_nodes):
            node = CrawlerNode(i)
            self.nodes.append(node)
            node_process = Process(target=node.crawl)
            node_process.start()

    def add_task(self, url):
        self.load_balancer.add_task(url)

class LoadBalancer:
    def __init__(self, nodes):
        self.nodes = nodes

    def add_task(self, url):
        healthiest_node = max(self.nodes, key=lambda node: node.health)
        healthiest_node.task_queue.put(url)
