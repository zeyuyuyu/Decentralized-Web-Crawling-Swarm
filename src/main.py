import random
import time
import json

class SwarmAgent:
    def __init__(self, id):
        self.id = id
        self.neighbors = []
        self.state = 'IDLE'
        self.task_queue = []

    def connect_to_neighbors(self, other_agents):
        for agent in other_agents:
            if agent.id != self.id:
                self.neighbors.append(agent)

    def broadcast_state(self):
        for neighbor in self.neighbors:
            neighbor.receive_state_update(self.state, self.task_queue)

    def receive_state_update(self, state, task_queue):
        self.state = state
        self.task_queue = task_queue

    def execute_task(self):
        if self.task_queue:
            task = self.task_queue.pop(0)
            print(f'Agent {self.id} executing task: {task}')
            time.sleep(random.uniform(1, 5))
            print(f'Agent {self.id} completed task: {task}')
        else:
            self.state = 'IDLE'

class SwarmCoordinator:
    def __init__(self, num_agents):
        self.agents = [SwarmAgent(i) for i in range(num_agents)]
        for agent in self.agents:
            agent.connect_to_neighbors(self.agents)

    def assign_tasks(self, tasks):
        for task in tasks:
            agent = self.find_available_agent()
            if agent:
                agent.task_queue.append(task)
                agent.state = 'WORKING'

    def find_available_agent(self):
        for agent in self.agents:
            if agent.state == 'IDLE':
                return agent
        return None

    def run_swarm(self):
        while True:
            for agent in self.agents:
                agent.execute_task()
                agent.broadcast_state()
            time.sleep(1)

if __name__ == '__main__':
    coordinator = SwarmCoordinator(10)
    tasks = ['Crawl website A', 'Scrape data from website B', 'Index content from website C']
    coordinator.assign_tasks(tasks)
    coordinator.run_swarm()
