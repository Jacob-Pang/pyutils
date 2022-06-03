import heapq
import os
import time

from collections.abc import Iterable
from importlib.util import spec_from_file_location, module_from_spec
from .wrappers import FunctionWrapper

class SchedulerCall (FunctionWrapper):
    def __init__(self, function: callable, freq: float, num_calls: int = None,
        expected_time_requirement: float = 0, call_message: str = None,
        **default_kwargs) -> None:

        super().__init__(function, **default_kwargs)
        self.freq = freq
        self.num_calls = num_calls
        self.expected_time_requirement = expected_time_requirement
        self.call_message = call_message
        self.call_count = 0
        self.scheduled_time = None

    def __call__(self, *args, **kwargs) -> any:
        if self.call_message:
            print(f"{time.time()}: {self.call_message}")

        out = super().__call__(*args, **kwargs)
        self.call_count += 1

        if self.num_calls and self.call_count >= self.num_calls:
            self.scheduled_time = None
        else: # Reschedule call
            self.scheduled_time = time.time() + self.freq

        return out

    def __lt__(self, other: object):
        # Priority given to calls with greater expected time requirement
        if self.call_count == 0 and other.call_count == 0:
            return self.expected_time_requirement > other.expected_time_requirement
        
        if self.call_count == 0:
            return True

        if other.call_count == 0:
            return False

        return self.scheduled_time < other.scheduled_time

def import_module_from_path(module_fpath: str):
    module_name = os.path.basename(module_fpath)
    module_spec = spec_from_file_location(module_name, module_fpath)
    module = module_from_spec(module_spec)
    module_spec.loader.exec_module(module)

    return module

def scheduler_call_wrapper(method: callable, freq: float, num_calls: int = None,
    expected_time_requirement: float = 0, call_message: str = None,
    **default_kwargs) -> callable:

    return SchedulerCall(method, freq=freq, **default_kwargs)

def run_scheduler(scheduler_calls: Iterable, **override_kwargs) -> None:
    schedule = heapq.heapify(scheduler_calls)

    while schedule:
        next_scheduler_call = heapq.heappop(schedule)

        if next_scheduler_call.scheduled_time:
            time.sleep(max(next_scheduler_call.scheduled_time - time.time(), 0))

        next_scheduler_call(**override_kwargs)

        if next_scheduler_call.scheduled_time:
            heapq.heappush(next_scheduler_call)

def run_scheduler_on_modules(modules: any, **override_kwargs):
    scheduler_calls = []

    for module in modules:
        if isinstance(module, str):
            module = import_module_from_path(module)

        for method in module.__dict__.values():
            if not isinstance(method, SchedulerCall):
                continue

            scheduler_calls.append(method)

    run_scheduler(scheduler_calls, **override_kwargs)

if __name__ == "__main__":
    pass
