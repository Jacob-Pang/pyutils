import time
import heapq
import random

from collections.abc import Iterable
from pyutils.wrappers import FunctionWrapper

class PyTask (FunctionWrapper):
    def __init__(self, function: callable, task_id: str = None, scheduled_timestamp: int = time.time(),
        freq: int = None, task_count: int = None, max_retries: int = 0, request_provider_usage: dict = {},
        **default_kwargs):
        """
        Parameters:
            function (callable): The function to call in order to run the task.
            task_id (str): Unique ID to identify the task with. If unspecified, a random ID will be
                    generated for the task.
            scheduled_timestamp (int): Scheduled time to run the task.
            freq (int): The frequency in seconds by which to run the task. If unspecified (None),
                    the task runs only once. Frequency does not account for the duration spent on
                    execution: if the frequency is 60s and a task takes 10s, the task is rescheduled
                    50s after the execution is completed. If the execution time exceeds the frequency
                    then the task is rescheduled 5s after the execution is completed.
            task_count (int): The number of times to run the task. If unspecified (None), the task will
                    be run indefinitely.
            provider_id (str): The request provider id to track runs.
            max_retries (int, opt): The number of times to retry the task in the event of exceptions.
                    Re-attempts are reset upon successful completion of the task.
            request_provider_usage (dict): The mapping of { RequestProvider: requests }, where
                    requests is the number of requests consumed per execution.
        """
        FunctionWrapper.__init__(self, function, **default_kwargs)

        if not task_id:
            task_id = f"pytask_{int(time.time())}_{int(random.random() * 1e5)}"
        
        self.task_id = task_id
        self.scheduled_timestamp = scheduled_timestamp
        self.freq = freq
        self.task_count = task_count
        self.request_provider_usage = request_provider_usage
        self.max_retries = max_retries

        self.retry_count = 0
        self.blocked_count = 0

    def __call__(self, *args, **kwargs) -> any:
        allocated_gates = dict()

        for request_provider, requests in self.request_provider_usage.items():
            gate, timestamp = request_provider.get_next_available_timestamp(requests)

            if timestamp > time.time():
                self.scheduled_timestamp = timestamp # Reschedule task
                self.blocked_count += 1
                return
            
            allocated_gates[gate] = requests

        for gate, requests in allocated_gates.items(): # process requests
            gate.process_requests(self.task_id, requests)    

        # Default settings in event of undefined response or exception.
        reschedule_task, gate_usage, task_success = True, True, False
        execution_time = 0

        try:
            gate_keys = { gate.gate_id: gate.gate_keys for gate in allocated_gates }
            start_time = time.time()
            task_output = FunctionWrapper.__call__(self, *args, gate_keys=gate_keys, **kwargs)

            execution_time = time.time() - start_time
            task_success = True

            if self.task_count: # Decrement count
                self.task_count -= 1

            if isinstance(task_output, tuple):
                reschedule_task, gate_usage = task_output
            elif task_output:
                reschedule_task, gate_usage = bool(task_output), True

            # Reschedule where the following conditions are met:
            # 1. frequency has been defined.
            # 2. task_count has not been defined (None) or task_count is greater than 0.
            # 3. task_output has not been defined (None) or task_output is True.
            reschedule_task = reschedule_task and self.freq and (self.task_count is None or self.task_count > 0)

        except Exception as task_exception:
            if self.retry_count >= self.max_retries:
                raise task_exception
            
            self.retry_count += 1

        for gate, _ in allocated_gates.items(): # complete requests and update usage capacity.
            gate.complete_requests(self.task_id, gate_usage)
        
        if reschedule_task:
            print("rescheduled??")
            self.scheduled_timestamp = time.time() + (
                max(self.freq - execution_time, 5) if task_success else 10
            )
                
        # Reset tracking parameters.
        self.blocked_count = 0
        self.retry_count = 0

        return task_success

    def __lt__(self, other: object):
        current_timestamp = time.time()

        if (self.scheduled_timestamp <= current_timestamp and other.scheduled_timestamp <= current_timestamp) \
            or self.scheduled_timestamp == other.scheduled_timestamp:
            return self.blocked_count > other.blocked_count    
        
        return self.scheduled_timestamp == other.scheduled_timestamp

def run_pytasks_scheduler(pytasks: Iterable, verbose: bool = True, **kwargs) -> None:
    pytasks = sorted(pytasks)
    heapq.heapify(pytasks)

    while pytasks:
        pytask = heapq.heappop(pytasks)

        while pytask.scheduled_timestamp > time.time():
            time.sleep(1) # Busy waiting

        task_success = pytask(**kwargs)

        if task_success and verbose:
            print(f"{int(time.time())}: successfully completed {pytask.task_id}.")

        if pytask.scheduled_timestamp: # Has rescheduled timing
            heapq.heappush(pytasks, pytask) # Requeue

            if verbose:
                print(f"{int(time.time())}: {pytask.task_id} rescheduled at {int(pytask.scheduled_timestamp)}.")

if __name__ == "main":
    pass
