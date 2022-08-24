import time

from multiprocessing import Semaphore

from pyutils import _STATE
from pyutils import StateNamespace
from pyutils import generate_unique_id
from pyutils.io import temporary_print
from pyutils.scheduler_legacy.task import Task
from pyutils.scheduler_legacy.task_queue import TaskQueue

class Worker:
    def __init__(self, scheduler: TaskQueue, worker_id: str = None, timeout: int = 30) -> None:
        """
        Parameters:
            scheduler (Scheduler): The scheduler assigning tasks to the worker.
            worker_id (str, opt): The ID assigned to the worker.
            timeout (int, opt): The number of seconds in IDLE state before the worker is retired.
        """
        if not worker_id: worker_id = generate_unique_id()

        self.worker_id = worker_id
        self.scheduler = scheduler
        self.timeout = timeout
        self.task_id = None
    
    def retire(self) -> None:
        self.scheduler.acquire()
        self.scheduler.retire_worker(self.worker_id)
        self.scheduler.print_state()
        self.scheduler.release()

    def execute_task(self, task: Task) -> None:
        self.task_id = task.task_id
        resources_freed = task()

        self.scheduler.acquire()

        if resources_freed:
            self.scheduler.update(task.resource_usage.keys())

        self.scheduler.push(task)
        self.scheduler.print_state()
        self.scheduler.release()
        self.task_id = None

    def __hash__(self) -> int:
        return self.worker_id.__hash__()

    def __str__(self) -> str:
        return f"Worker {self.worker_id} [ STATUS : {(f'RUNNING ({self.task_id})' if self.task_id else 'IDLE'):<25} ]"

    def run(self) -> None:
        timeout = time.time() + self.timeout

        while time.time() < timeout:
            self.scheduler.acquire()

            if not self.scheduler.active():
                self.scheduler.release()
                break

            if not self.scheduler.has_active_tasks() or not self.scheduler.has_pending_task():
                self.scheduler.release()
                continue

            task = self.scheduler.pop()
            self.scheduler.release()

            self.execute_task(task)
            timeout = time.time() + self.timeout
        
        self.retire()

class Scheduler (TaskQueue):
    def __init__(self, *tasks: Task, timeout: int = 30, semaphore: Semaphore = None,
        state: StateNamespace = _STATE) -> None:
        super().__init__(*tasks, semaphore=semaphore)

        self.timeout = timeout
        self.state = state
        self.verbose_mode = True

        self.state.set_attr("workers", dict(), override_attr=False)
        self.state.set_attr("resources", dict(), override_attr=False)
        self.state.set_attr("tasks", dict(), override_attr=False)

        for task in self.tasks:
            self.state.tasks[task.task_id] = task

        for task in self.state.tasks.values():
            self.update_resources(task)

    def update_resources(self, task: Task) -> None:
        for resource in task.resource_usage:
            self.state.resources[resource.resource_id] = resource

    def push(self, task: Task) -> None:
        if not task.task_id in self.state.tasks:
            self.state.tasks[task.task_id] = task
            self.update_resources(task)

        return super().push(task)

    def create_worker(self) -> Worker:
        worker = Worker(scheduler=self, worker_id=generate_unique_id(self.state), timeout=self.timeout)
        self.state.workers[worker.worker_id] = worker
        
        return worker

    def retire_worker(self, worker_id: str) -> None:
        self.state.workers.pop(worker_id)

    def state_repr(self) -> str:
        return f"Timestamp : {time.time()}" + \
                "\n=============================================================================================================\n" + \
                "Resources\n\n" + \
                "\n".join([str(resource) for resource in self.state.resources.values()]) + \
                "\n\n=============================================================================================================\n" + \
                "Workers\n\n" + \
                "\n".join([str(worker) for worker in self.state.workers.values()]) + \
                "\n\n=============================================================================================================\n" + \
                "Tasks\n\n" + \
                "\n".join([str(task) for task in self.state.tasks.values()]) + "\n\n"

    def print_state(self) -> None:
        if self.verbose_mode:
            temporary_print(self.state_repr(), self.state)

    def run(self) -> None:
        while self.active():
            worker = self.create_worker()
            worker.run()

if __name__ == "__main__":
    pass