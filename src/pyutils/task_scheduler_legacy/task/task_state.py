import time

class TaskState:
    NEW_STATE     = "NEW"
    WAITING_STATE = "WAITING"
    RUNNING_STATE = "RUNNING"
    DONE_STATE    = "DONE"
    EXCEPT_STATE  = "EXCEPT"
    BLOCKED_STATE = "BLOCKED"

    def __init__(self, name: str, state: str, run_count: int, timestamp: float = None) -> None:
        if not timestamp:
            timestamp = time.time()
        
        self.name = name
        self.state = state
        self.run_count = run_count
        self.timestamp = timestamp

    def __repr__(self) -> str:
        return f"TASK  {str(self.name):<36} [ {self.state:<10} : {int(self.timestamp):<15} | {self.run_count:<4} ]"

if __name__ == "__main__":
    pass