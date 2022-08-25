import time

from multiprocessing.managers import Namespace
from pyutils.scheduler.worker.worker_state import BusyState, IdleState, DeadState

class Worker:
    def __init__(self, key: str, master_process_state: Namespace, timeout: int = None) -> None:
        self.key = key
        self.timeout = timeout
        self.master_process_state = master_process_state

    def __call__(self) -> None:
        start_time = time.time()
        task_manager = self.master_process_state.task_manager

        while self.heartbeat(start_time):
            with task_manager.semaphore:
                task = task_manager.dispatch_task()

                if not task: # No actionable tasks.
                    continue

                task_manager.update_worker_state(BusyState(self.key, task.key))
            
            task_state = task()

            with task_manager.semaphore:
                task_manager.post_update(task, task_state)
                task_manager.update_worker_state(IdleState(self.key))

            start_time = time.time()

        self.stop()

    def heartbeat(self, start_time: float) -> bool:
        if not self.master_process_state.active: # MasterProcess has been shut down.
            return False

        if not self.master_process_state.task_manager.state.public_pending_tasks and \
            not self.master_process_state.listening_mode:
            return False # No public_pending_tasks and not on listening_mode.

        if self.timeout: # Timeout reached
           return time.time() <= start_time + self.timeout

        return True

    def stop(self) -> None:
        task_manager = self.master_process_state.task_manager

        with task_manager.semaphore:
            task_manager.update_worker_state(DeadState(self.key))

if __name__ == "__main__":
    pass