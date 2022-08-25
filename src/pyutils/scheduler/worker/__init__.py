import time

from multiprocessing.managers import Namespace
from pyutils.scheduler.worker.worker_state import BusyState, IdleState, DeadState

class Worker:
    def __init__(self, key: str, master_process_state: Namespace, timeout: int = None,
        remove_worker_state_on_death: bool = True) -> None:

        self.key = key
        self.timeout = timeout
        self.master_process_state = master_process_state
        self.remove_worker_state_on_death = remove_worker_state_on_death

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
        if self.timeout and time.time() <= start_time + self.timeout: # Timeout reached
           return False

        if not self.master_process_state.active: # MasterProcess has been shut down.
            return False

        if not self.master_process_state.task_manager.state.public_pending_tasks and \
            not self.master_process_state.listening_mode:
            return False # No public_pending_tasks and not on listening_mode.

        return True

    def stop(self) -> None:
        task_manager = self.master_process_state.task_manager

        with task_manager.semaphore:
            task_manager.update_worker_state(
                DeadState(self.key, remove_worker_state=self.remove_worker_state_on_death)
            )

if __name__ == "__main__":
    pass