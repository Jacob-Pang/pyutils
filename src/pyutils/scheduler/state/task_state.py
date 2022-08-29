import time

class TaskState:
    def __init__(self, key: str, run_count: int, retry_attempts: int, visible: bool, private: bool,
        timestamp: float = None) -> None:

        if timestamp is None:
            timestamp = time.time()

        self.key = key
        self.run_count = run_count
        self.retry_attempts = retry_attempts
        self.visible = visible
        self.private = private
        self.timestamp = timestamp

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
    def __init__(self, resource_units: dict, key: str, run_count: int, retry_attempts: int,
        visible: bool, private: bool, timestamp: float = None) -> None:

        super().__init__(key, run_count, retry_attempts, visible, private, timestamp)
        self.resource_units = resource_units

    def get_run_count(self) -> int:
        return self.run_count + 1

    def get_state_repr(self) -> str:
        return "RUNNING"

class BlockedState (TaskState):
    def __init__(self, resource_constraints: set, key: str, run_count: int, retry_attempts: int,
        visible: bool, private: bool, timestamp: float = None) -> None:

        super().__init__(key, run_count, retry_attempts, visible, private, timestamp)
        self.resource_constraints = resource_constraints

    def get_run_count(self) -> int:
        return self.run_count + 1

    def get_state_repr(self) -> str:
        return f"BLOCKED"

class DoneState (TaskState):
    def __init__(self, remove_task_state: bool, tasks_to_register: dict, key: str, run_count: int,
        retry_attempts: int, visible: bool, private: bool, timestamp: float = None) -> None:

        super().__init__(key, run_count, retry_attempts, visible, private, timestamp)
        self.remove_task_state = remove_task_state
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