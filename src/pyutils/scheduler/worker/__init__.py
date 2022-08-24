import time

from multiprocessing.managers import Namespace
from pyutils.scheduler.worker.worker_state import BusyState, IdleState, DeadState

class Worker:
    def __init__(self, key: str, task_scheduler_state: Namespace, timeout: int = 30) -> None:
        self.key = key
        self.timeout = timeout
        self.task_scheduler_state = task_scheduler_state

    def __call__(self) -> None:
        timeout_time = time.time() + self.timeout
        task_manager = self.task_scheduler_state.task_manager

        while time.time() < timeout_time:
            if not self.task_scheduler_state.active:
                break # Scheduler has shut down

            if not task_manager.active_tasks() and not self.task_scheduler_state.listening:
                break # No more active tasks and not listening for new tasks

            with task_manager.semaphore:
                task = task_manager.dispatch_task()
                if not task: continue

                task_manager.update_worker_state(BusyState(self.key, task.key))
            
            task_state = task()

            with task_manager.semaphore:
                task_manager.post_update(task, task_state)
                task_manager.update_worker_state(IdleState(self.key))

                timeout_time = time.time() + self.timeout
        
        with task_manager.semaphore:
            task_manager.update_worker_state(DeadState(self.key))

if __name__ == "__main__":
    pass