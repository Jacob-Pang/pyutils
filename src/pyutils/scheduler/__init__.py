import time

from multiprocessing import Semaphore
from pyutils import generate_unique_id
from pyutils.scheduler.task import Task
from pyutils.scheduler.task_queue import TaskQueue

class Worker:
    def __init__(self, manager: TaskQueue, worker_id: str = None, timeout: int = 30) -> None:
        """
        Parameters:
            manager (Manager): The manager scheduling work to the worker.
            worker_id (str, opt): The ID assigned to the worker.
            timeout (int, opt): The number of seconds in IDLE state before the worker is retired.
        """
        if not worker_id: worker_id = generate_unique_id()

        self.worker_id = worker_id
        self.manager = manager
        self.timeout = timeout
        self.task = None
    
    def retire(self) -> None:
        self.manager.acquire()
        self.manager.retire_worker(self.worker_id)
        self.manager.release()

    def execute_task(self, task: Task) -> None:
        resources_freed = task()

        self.manager.acquire()

        if resources_freed:
            self.manager.update(task.resource_usage.keys())

        self.manager.push(task)
        self.manager.release()

    def __hash__(self) -> int:
        return self.worker_id.__hash__()

    def run(self) -> None:
        timeout = time.time() + self.timeout

        while time.time() < timeout:
            self.manager.acquire()

            if not self.manager.tasks or self.manager.peek().scheduled_time > time.time():
                self.manager.release()
                continue

            self.manager.acquire()
            task = self.manager.pop()
            self.manager.release()

            self.execute_task(task)
            timeout = time.time() + self.timeout
            
        self.retire()

class Manager (TaskQueue):
    def __init__(self, *tasks: Task, timeout: int = 30, semaphore: Semaphore = None) -> None:
        super().__init__(*tasks, semaphore=semaphore)

        self.timeout = timeout
        self.workers = set()

    def create_worker(self) -> Worker:
        worker = Worker(manager=self, timeout=self.timeout)
        self.workers.add(worker)
        
        return worker

    def retire_worker(self, worker) -> None:
        self.workers.remove(worker)

    def run_sync(self) -> None:
        while self: # Exisiting tasks
            worker = self.create_worker()
            worker.run()

if __name__ == "__main__":
    pass