import time
from pyutils.scheduler.task import Task

class TaskState:
    # Sanpshot of the state of the task.
    def __init__(self, task: Task, timestamp: float = None) -> None:
        if timestamp is None: timestamp = time.time()

        self.key = task.key
        self.timestamp = timestamp
        
        self.run_count = task.run_count
        self.retry_attempts = task.retry_attempts

        self.private_mode = task.private_mode

    def get_run_count(self) -> int:
        # Interpretation of run_count based on state
        return self.run_count

    def get_state_repr(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"TASK     | {self.key:<43} [ {self.get_state_repr():<9} | {int(self.timestamp):<15} |" + \
                f" {self.get_run_count():<4} ]"

class NewState (TaskState):
    def get_run_count(self) -> int:
        return self.run_count + 1

    def get_state_repr(self) -> str:
        if self.retry_attempts:
            return f"RETRY ({self.retry_attempts})"
        
        return "NEW"

class RunningState (TaskState):   
    def __init__(self, task: Task, resource_units: dict, timestamp: float = None) -> None:
        super().__init__(task, timestamp)
        self.resource_units = resource_units

    def get_run_count(self) -> int:
        return self.run_count + 1

    def get_state_repr(self) -> str:
        return "RUNNING"

class BlockedState (TaskState):
    def __init__(self, task: Task, resource_constraints: set, timestamp: float = None) -> None:
        super().__init__(task, timestamp)
        self.resource_constraints = resource_constraints

    def get_run_count(self) -> int:
        return self.run_count + 1

    def get_state_repr(self) -> str:
        return f"BLOCKED"

class DoneState (TaskState):
    def __init__(self, task: Task, tasks_to_register: dict, timestamp: float = None) -> None:
        super().__init__(task, timestamp)
        self.remove_task_state = task.remove_task_state_on_done
        self.tasks_to_register = tasks_to_register

    def get_state_repr(self) -> str:
        return "DONE"

class ExceptionState (DoneState):
    def __repr__(self) -> str:
        return "EXCEPTION"

class WaitingState (RunningState):
    def get_state_repr(self) -> str:
        return "WAITING"

if __name__ == "__main__":
    pass