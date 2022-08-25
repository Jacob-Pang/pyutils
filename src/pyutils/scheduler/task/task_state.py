import time

class TaskState:
    def __init__(self, key: str, timestamp: float = None, private_mode: bool = False) -> None:
        if timestamp is None: timestamp = time.time()

        self.key = key
        self.timestamp = timestamp
        self.private_mode = private_mode

    def get_state_repr(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f"TASK     | {self.key:<43} [ {self.get_state_repr():<9} | {int(self.timestamp):<15} ]"

class NewState (TaskState):
    def get_state_repr(self) -> str:
        return "NEW"

class RunningState (TaskState):   
    def __init__(self, key: str, resource_units: dict, timestamp: float = None, private_mode: bool = False) -> None:
        super().__init__(key, timestamp, private_mode)
        self.resource_units = resource_units

    def get_state_repr(self) -> str:
        return "RUNNING"

class DoneState (TaskState):
    def __init__(self, key: str, timestamp: float = None, private_mode: bool = False,
        remove_task_state: bool = True) -> None:

        super().__init__(key, timestamp, private_mode)
        self.remove_task_state = remove_task_state

    def get_state_repr(self) -> str:
        return "DONE"

class ExceptionState (DoneState):
    def __repr__(self) -> str:
        return "EXCEPTION"

class BlockedState (TaskState):
    def __init__(self, key: str, resource_constraints: set, timestamp: float = None, private_mode: bool = False) -> None:
        super().__init__(key, timestamp, private_mode)
        self.resource_constraints = resource_constraints

    def get_state_repr(self) -> str:
        return f"BLOCKED"

class WaitingState (RunningState):
    def get_state_repr(self) -> str:
        return "WAITING"

if __name__ == "__main__":
    pass