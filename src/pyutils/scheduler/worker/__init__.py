import time

from multiprocessing.managers import Namespace
from pyutils.scheduler.task_manager import TaskManager
from pyutils.scheduler.task.task_state import DoneState, ExceptionState
from pyutils.scheduler.worker.worker_state import BusyState, IdleState, DeadState

class Worker:
    def __init__(self, key: str, task_manager: TaskManager, master_process_state: Namespace,
        timeout: int = None, remove_worker_state_on_death: bool = True) -> None:

        self.key = key
        self.timeout = timeout
        self.task_manager = task_manager
        self.master_process_state = master_process_state
        self.remove_worker_state_on_death = remove_worker_state_on_death

    @property
    def active(self) -> bool:
        return self.master_process_state.active

    @property
    def listening_mode(self) -> bool:
        return self.master_process_state.listening_mode

    def __call__(self) -> None:
        start_time = time.time()

        while self.heartbeat(start_time):
            with self.task_manager.semaphore:
                task = self.task_manager.dispatch_task()

                if not task: # No actionable tasks.
                    continue

                self.task_manager.update_worker_state(BusyState(self.key, task.key))
            
            tasks_to_register = dict()
            task_state = DoneState(task, tasks_to_register) if task(tasks_to_register=tasks_to_register) else \
                    ExceptionState(task, tasks_to_register)
            
            with self.task_manager.semaphore:
                self.task_manager.post_update(task, task_state)
                self.task_manager.update_worker_state(IdleState(self.key))

            start_time = time.time()

        self.stop()

    def heartbeat(self, start_time: float) -> bool:
        if self.timeout and time.time() <= start_time + self.timeout: # Timeout reached
            return False

        if not self.active: # MasterProcess has been shut down.
            return False

        if not self.task_manager.public_pending_tasks and not self.listening_mode:
            return False # No public_pending_tasks and not on listening_mode.

        return True

    def stop(self) -> None:
        task_manager = self.task_manager

        with task_manager.semaphore:
            task_manager.update_worker_state(
                DeadState(self.key, remove_worker_state=self.remove_worker_state_on_death)
            )

if __name__ == "__main__":
    pass