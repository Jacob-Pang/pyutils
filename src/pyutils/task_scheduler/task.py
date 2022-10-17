import time
from types import SimpleNamespace
from uuid import uuid4
from pyutils.wrapper import WrappedFunction

class Task (WrappedFunction):
    def __init__(self, target_function: callable, key: str = None, resource_usage: dict = dict(),
        start_time: float = None, repeat_freq: float = 0., runs: int = 1, **kwargs: any):
        """ Parameters:
        :target_function (callable): The function to run.
        :key (str, opt): The key assigned to the Task.
        :resource_usage (dict, opt): The mapping of resource allocator aliases to units consumed on running.
        :start_time (float, opt): The time to start running the Task.
        :repeat_freq (float, opt): The frequency at which to repeat the Task.
        :runs (int, opt): The number of times to run the Task.

        Notes:
        a. Other options such as ignoring exceptions or retrying on exceptions are not implemented as they
                can be embedded within the target function or using another WrappedFunction layer.
        b. More complex behaviours can be achieved by modifying _task_state and _master_process during
                the execution of the Task. See Task.__call__
        """
        WrappedFunction.__init__(self, target_function, **kwargs)

        self.key = key if key else uuid4()
        self.resource_usage = resource_usage

        self._start_time = start_time if start_time else time.time()
        self._repeat_freq = repeat_freq
        self._runs = runs

    def __lt__(self, other) -> bool:
        return self._start_time < other._start_time

    # Accessors
    def get_time_to_ready(self) -> float:
        return max(time.time() - self._start_time, 0)

    def ready(self) -> bool:
        return self._start_time < time.time()

    def get_remaining_runs(self) -> int:
        return self._runs

    # Mutators
    def __call__(self, master_process, allocated_resources: dict) -> any:
        """ To support complex behaviours, the target function can accept any of the parameters
        _master_process, _allocated_resources and _task_state (note the prefix).

        1. Resource-dependent behaviour
            The arg _allocated_resources allows the target function to access the allocated resource keys.
            E.g. Suppose there the alias R points to two different resouces R1 and R2. The following target
            function can be configured to behave differently based on whether it receives R1 or R2:

            def target(_allocated_resources: dict) -> int:
                if _allocated_resources.get("R") == "R1":
                    return 0
                elif _allocated_resources.get("R") == "R2":
                    return 1
                
                return 2

            Note that only the key and not the resource object is passed.
        """
        _task_state = SimpleNamespace(
            repeat_freq=self._repeat_freq,
            reduce_runs=1,
            free_resources=True
        )

        output = super().__call__(
            _master_process=master_process,
            _allocated_resources=allocated_resources,
            _task_state=_task_state
        )
        
        # Update end of task
        self._runs -= _task_state.reduce_runs
        self._start_time += _task_state.repeat_freq

        if _task_state.free_resources:
            master_process.free_resources(self.resource_usage, allocated_resources)
        
        master_process.post_task_completion(self, output)
        return output

if __name__ == "__main__":
    pass