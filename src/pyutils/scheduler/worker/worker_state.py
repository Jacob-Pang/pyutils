import time

class WorkerState:
    def __init__(self, key: str, task_key: str = None, timestamp: float = None) -> None:
        if not timestamp: timestamp = time.time()

        self.key = key
        self.task_key = task_key
        self.timestamp = timestamp

    def get_state_repr(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        status = self.get_state_repr()
        task_key = self.task_key if self.task_key else "NaN"
        elapsed_time = int(time.time() - self.timestamp) if self.task_key else "NaN"

        return f"WORKER   | {self.key:<43} [ {status:<9} | {task_key:<25} | {elapsed_time:<15} ]"

class BusyState (WorkerState):
    def get_state_repr(self) -> str:
        return "BUSY"

class IdleState (WorkerState):
    def __init__(self, key: str, timestamp: float = None) -> None:
        super().__init__(key, None, timestamp)

    def get_state_repr(self) -> str:
        return "IDLE"

class DeadState (WorkerState):
    def __init__(self, key: str, timestamp: float = None) -> None:
        super().__init__(key, timestamp)
        
    def get_state_repr(self) -> str:
        return "DEAD"

if __name__ == "__main__":
    pass