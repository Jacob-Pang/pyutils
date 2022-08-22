import heapq

from collections.abc import Iterable
from multiprocessing import Semaphore
from pyutils.scheduler.task import Task, TaskState

class TaskQueue:
    def __init__(self, *tasks: Task, semaphore: Semaphore = None) -> None:
        if not semaphore: semaphore = Semaphore(1)
        
        self.tasks = tasks
        self.blocked_tasks = set()
        self.semaphore = Semaphore(1)
    
    def acquire(self) -> None:
        self.semaphore.acquire()
    
    def release(self) -> None:
        self.semaphore.release()

    def sort_queue(self) -> None:
        heapq.heapify(self.tasks)
    
    def push(self, task: Task) -> None:
        if self.task.state == TaskState.Blocked():
            return self.blocked_tasks.add(task)
        
        heapq.heappush(self.tasks, task)
        
    def pop(self) -> Task:
        heapq.heappop(self.tasks)

    def peek(self) -> Task:
        return self.tasks[0]

    def update(self, freed_resources: Iterable) -> None:
        # Attempt to unblock tasks with freed resources
        blocked_tasks = set()

        for blocked_task in self.blocked_tasks:
            if blocked_task.resource_constraint in freed_resources and blocked_task \
                .schedule_and_reserve_resources():    
                self.push(blocked_task)
            else:
                blocked_tasks.add(blocked_task)
        
        self.blocked_tasks = blocked_tasks

if __name__ == "__main__":
    pass    