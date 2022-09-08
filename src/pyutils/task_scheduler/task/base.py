import time

from uuid import uuid4

class TaskBase:
    def __init__(self, name: str = None, start_time: float = None, resource_usage: dict = None,
        runs: int = 1, repeat_freq: float = 0, retry_on_except: int = 0, raise_on_except: bool = True,
        remove_on_done: bool = True) -> None:
        """
        Parameters:
        :name (str, opt): The name of the task.
        :start_time (float, opt): The time at which to run the task. Defaults to current time.
        :resource_usage (dict, opt): The mapping of resource_key: capacity_usage (see Resource).
        :runs (int, opt): The number of runs to run the task for. Set to negative values to run
                the task indefinitely.
        :repeat_freq (float, opt): The frequency at which to repeat the task, accounting for any
                execution time.
        :retry_on_except (int, opt): The number of attempts to retry the task on exceptions.
        :raise_on_except (bool, opt): Whether to propagate any exceptions.
        :remove_on_done (bool, opt): Whether to remove the task from TaskManager on completion.
        """
        self.key = uuid4()

        if not name:
            name = self.key

        if not start_time:
            start_time = time.time()

        if not resource_usage:
            resource_usage = dict()
            
        self.name = name
        self.start_time = start_time
        self.resource_usage = resource_usage
        self.runs = runs
        self.repeat_freq = repeat_freq
        
        self._retry_on_except = retry_on_except
        self._raise_on_except = raise_on_except
        self._remove_on_done = remove_on_done
        self._run_count = 0
        self._retry_count = 0

if __name__ == "__main__":
    pass