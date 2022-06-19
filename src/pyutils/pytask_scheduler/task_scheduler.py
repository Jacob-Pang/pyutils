import time
import heapq
import random

from ..wrappers import FunctionWrapper
from .request_provider import RequestProviderManager

class PyTask (FunctionWrapper):
    def __init__(self, function: callable, task_id: str = None, scheduled_time: int = time.time(),
        freq: int = None, task_count: int = None, request_provider_usage: dict = {}, **default_kwargs):
        """
        Parameters:
            function (callable): The function to call in order to run the task.
            task_id (str): Unique ID to identify the task with. If unspecified, a random ID will be
                    generated for the task.
            scheduled_time (int): Scheduled time to run the task.
            freq (int): The frequency in seconds by which to run the task. If unspecified (None),
                    the task runs only once.
            task_count (int): The number of times to run the task. If unspecified (None), the task will
                    be run indefinitely.
            provider_id (str): The request provider id to track runs.
            request_provider_usage (dict): The mapping of { request_provider_id: request_counts }, where
                    request_counts is the number of requests consumed per execution.
        """
        FunctionWrapper.__init__(self, function, **default_kwargs)
        self.task_id = f"pytask_{int(random.random() * 1e5)}_{int(time.time())}" \
                if not task_id else task_id
        
        self.scheduled_time = scheduled_time
        self.freq = freq
        self.task_count = task_count
        self.request_provider_usage = request_provider_usage
        self.blocked_count = 0

    def __call__(self, *args, **kwargs) -> any:
        if self.task_count:
            self.task_count -= 1

        task_output = FunctionWrapper.__call__(self, *args, **kwargs)
        self.scheduled_time = time.time() + self.freq if self.freq and (self.task_count is None
                or self.task_count > 0) else None
        
        self.blocked_count = 0
        return task_output

    def schedule(self, scheduled_time: int) -> None:
        if scheduled_time > time.time(): # Blocking reschedule.
            self.blocked_count += 1
        
        self.scheduled_time = scheduled_time
        
    def __lt__(self, other: object):
        if self.scheduled_time < other.scheduled_time:
            return True
        
        return self.blocked_count > other.blocked_count

class PyTaskScheduler:
    def __init__(self, request_provider_manager: RequestProviderManager = RequestProviderManager()):
        self.request_provider_manager = request_provider_manager

    def run_tasks(self, tasks: list, reschedule_verbose: bool = True, **kwargs) -> None:
        tasks = sorted(tasks)
        heapq.heapify(tasks)

        while tasks:
            task = heapq.heappop(tasks)

            while task.scheduled_time > time.time():
                time.sleep(1) # Busy waiting
            
            # Schedule the tasks
            scheduled_time, request_providers, constraint_request_provider_id = self \
                    .request_provider_manager.schedule_requests(task.request_provider_usage)

            task.schedule(scheduled_time)

            if scheduled_time > time.time():
                heapq.heappush(tasks, task) # Requeue blocked task
                if reschedule_verbose:
                    print(f"{int(time.time())}: RequestProvider: {constraint_request_provider_id}",
                            f"reached maximum requests limit. Task: {task.task_id[:15]:<15}",
                            f"rescheduled to {int(scheduled_time)}")
                
                continue

            print(f"{int(time.time())}: Running task: {task.task_id[:15]:<15} ... ", end='')
            api_keys = {}

            for request_provider_id, request_provider in request_providers.items():
                api_keys[request_provider_id] = request_provider.api_key
                print("request_provider", request_provider_id, "running requests", task.request_provider_usage \
                            .get(request_provider_id)) # TO REMOVE
                            
                request_provider.run_requests(task.task_id, task.request_provider_usage \
                            .get(request_provider_id))
                
            task(api_keys=api_keys, **kwargs) # Run task
            print(f"completed at {int(time.time())}.")

            for request_provider_id, request_provider in request_providers.items():
                request_provider.complete_requests(task.task_id)

            if task.scheduled_time:
                heapq.heappush(tasks, task) # Requeue blocked task

                if reschedule_verbose:
                    print(f"{int(time.time())}: Task: {task.task_id[:15]:<15}",
                            f"rescheduled to {int(task.scheduled_time)}")

            print(f"PyTask scheduler tasks remaining: {len(tasks)}")

if __name__ == "main":
    pass
