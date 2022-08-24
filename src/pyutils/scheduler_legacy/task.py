import time

from pyutils import generate_unique_id
from pyutils.wrappers import WrappedFunction
from pyutils.scheduler_legacy.resource import Resource

def predicate_never(output: any):
    return False

def predicate_always(output: any):
    return True

class TaskState:
    class TaskStateBase:
        def __repr__(self) -> str:
            raise NotImplementedError()
        
        def __eq__(self, other: object) -> bool:
            return isinstance(other, self.__class__)

    class Ready (TaskStateBase):
        def __repr__(self) -> str:
            return "READY"

    class Delayed (TaskStateBase):
        def __repr__(self) -> str:
            return "DELAYED"

    class Blocked (TaskStateBase):
        def __repr__(self) -> str:
            return "BLOCKED"

    class Running (TaskStateBase):
        def __repr__(self) -> str:
            return "RUNNING"

    class Error (TaskStateBase):
        def __repr__(self) -> str:
            return "ERROR"

    class Done (TaskStateBase):
        def __repr__(self) -> str:
            return "DONE"

class Task (WrappedFunction):
    def __init__(self, method: callable, task_id: str = None, resource_usage: dict = dict(),
        scheduled_time: int = time.time(), reschedule_freq: int = 0, retry_on_except: int = 0,
        raise_on_except: bool = True, reschedule_pred: callable = predicate_never, **default_kwargs):
        
        super().__init__(method, **default_kwargs)

        self.task_id = task_id if task_id else generate_unique_id()
        self.resource_usage  = resource_usage
        self.scheduled_time  = scheduled_time
        self.reschedule_freq = reschedule_freq
        self.retry_on_except = retry_on_except
        self.raise_on_except = raise_on_except
        self.reschedule_pred = reschedule_pred

        self.state = TaskState.Ready()
        self.reserved_gates = None # {ResourceGate: usage}
        self.resource_constraint = None
        self.retry_count = 0
        self.completed_count = 0

    def block(self, resource_constraint: Resource) -> None:
        self.scheduled_time = None
        self.reserved_gates = None
        self.resource_constraint = resource_constraint
        self.state = TaskState.Blocked()

    def schedule_and_reserve_resources(self) -> bool:
        # Returns whether reservation was performed successfully.
        self.scheduled_time, self.reserved_gates = 0, dict()

        for resource, units in self.resource_usage.items():
            free_timestamp, acquired_gate = resource.acquire_free_gate_and_time(units)

            if not acquired_gate:
                for gate in self.reserved_gates:
                    gate.release()

                self.block(resource)
                return False
            
            self.reserved_gates[acquired_gate] = units
            self.scheduled_time = max(self.scheduled_time, free_timestamp)

        for gate, units in self.reserved_gates.items():
            gate.reserve(units)
            gate.release()

        self.resource_constraint = None
        self.state = TaskState.Ready()
        return True

    def reschedule(self, start_time: int) -> None:
        # Reschdules timing without scheduling resource usage
        self.scheduled_time = max(start_time + self.reschedule_freq, time.time())

    def __call__(self, *args, **kwargs) -> bool:
        # Returns whether resources were freed.
        if not self.reserved_gates: # No prior reservations made
            if not self.schedule_and_reserve_resources():
                return False # Blocked indefinitely.

        if self.scheduled_time > time.time():
            self.state = TaskState.Delayed()
            return False

        # Transit to running phase
        self.scheduled_time = None
        self.state = TaskState.Running()
        start_time = time.time()

        for gate, units in self.reserved_gates.items():
            gate.acquire()
            gate.use(units)
            gate.release()

        try:
            output = super().__call__(*args, **kwargs)
            self.completed_count += 1

            if self.reschedule_pred(output):
                self.reschedule(start_time)
                self.state = TaskState.Ready()
            else:
                self.state = TaskState.Done()

        except Exception as error:
            self.state = TaskState.Error()

            if self.retry_count >= self.retry_on_except:
                if self.raise_on_except:
                    raise error
            else:
                self.retry_count += 1
                self.reschedule(start_time)

        # Free usage units and update gate
        for gate, units in self.reserved_gates.items():
            gate.acquire()
            gate.free(units)
            gate.update()
            gate.release()

        self.reserved_gates = None
        return True

    def __repr__(self) -> str:
        return f"Task {self.task_id:<25} [ Status : {self.state.__repr__():<7} | Scheduled : " + \
                f"{(int(self.scheduled_time) if self.scheduled_time else 'None'):<10} | " + \
                f"Completed : {self.completed_count:<4} ]"

    def __lt__(self, other: object) -> bool:
        if self.scheduled_time is None:  return False
        if other.scheduled_time is None: return True

        return self.scheduled_time < other.scheduled_time

if __name__ == "__main__":
    pass