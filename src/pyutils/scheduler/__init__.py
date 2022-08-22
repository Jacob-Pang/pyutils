import time

from multiprocessing import Semaphore
from pyutils import generate_unique_id
from pyutils.io_utils import temporary_print
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
        self.task_id = None
    
    def retire(self) -> None:
        self.manager.acquire()
        self.manager.retire_worker(self.worker_id)
        self.manager.update_state()
        self.manager.release()

    def execute_task(self, task: Task) -> None:
        self.task_id = task.task_id
        resources_freed = task()

        self.manager.acquire()

        if resources_freed:
            self.manager.update(task.resource_usage.keys())

        self.manager.push(task)
        self.manager.update_state()
        self.manager.release()
        self.task_id = None

    def __hash__(self) -> int:
        return self.worker_id.__hash__()

    def __str__(self) -> str:
        return f"Worker {self.worker_id} [ STATUS : {(f'RUNNING ({self.task_id})' if self.task_id else 'IDLE'):<25} ]"

    def run(self) -> None:
        timeout = time.time() + self.timeout

        while time.time() < timeout:
            self.manager.acquire()

            if self.manager.done():
                self.manager.release()
                break

            if not self.manager.has_active_tasks() or not self.manager.has_pending_task():
                self.manager.release()
                continue

            task = self.manager.pop()
            self.manager.release()

            self.execute_task(task)
            timeout = time.time() + self.timeout
        
        self.retire()

class TaskManager (TaskQueue):
    def __init__(self, *tasks: Task, timeout: int = 30, semaphore: Semaphore = None) -> None:
        super().__init__(*tasks, semaphore=semaphore)

        self.timeout = timeout
        self.workers = dict()
        self.resources = set()
        self.listening_mode = False

        for task in self.tasks:
            for resource in task.resource_usage:
                self.resources.add(resource)

    def push(self, task: Task) -> None:
        for resource in task.resource_usage:
            self.resources.add(resource)

        return super().push(task)

    def create_worker(self) -> Worker:
        worker = Worker(manager=self, timeout=self.timeout)
        self.workers[worker.worker_id] = worker
        
        return worker

    def retire_worker(self, worker_id: str) -> None:
        self.workers.pop(worker_id)

    def state_repr(self) -> str:
        return f"Timestamp : {time.time()}\n" + \
                "\n===============================================================================================\n" + \
                "Resources\n\n" + \
                "\n".join([str(resource) for resource in self.resources]) + \
                "\n===============================================================================================\n" + \
                "Workers\n\n" + \
                "\n".join([str(worker) for worker in self.workers]) + \
                "\n===============================================================================================\n" + \
                "Tasks\n" + \
                "\n".join([str(task) for task in self.tasks]) + "\n" + \
                "\n".join([str(task) for task in self.blocked_tasks]) + "\n\n"

    def update_state(self) -> None:
        temporary_print(self.state_repr())

    def run_sync(self) -> None:
        self.listening_mode = False

        while not self.done():
            worker = self.create_worker()
            worker.run()

    def done(self) -> bool:
        return super().done() or self.listening_mode

if __name__ == "__main__":
    pass