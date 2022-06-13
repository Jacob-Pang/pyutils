import time
import heapq
import random

from ..wrappers import FunctionWrapper

class PyTask (FunctionWrapper):
    def __init__(self, function: callable, task_id: str = None,
        scheduled_time: int = time.time(), freq: int = None, task_count: int = None,
        request_provider_id_to_count: dict = {}, **default_kwargs):
        """
        Parameters:
            function (callable): The function to call in order to run the task.
            task_id (str): Unique ID to identify the task with. If unspecified,
                    a random ID will be generated for the task.
            scheduled_time (int): Scheduled time to run the task.
            freq (int): The frequency in seconds by which to run the task. If
                    unspecified (None), the task runs only once.
            task_count (int): The number of times to run the task. If unspecified
                    (None), the task is repeatedly run indefinitely.
            provider_id (str): The request provider id to track runs.
            request_provider_id_to_count (dict): The mapping of request
                    provider ID to the number of requests consumed for the task.
        """
        FunctionWrapper.__init__(self, function, **default_kwargs)
        self.task_id = f"pytask_{int(random.random() * 1e5)}_{int(time.time())}" \
                if not task_id else task_id
        
        self.scheduled_time = scheduled_time
        self.freq = freq
        self.task_count = task_count
        self.request_provider_id_to_count = request_provider_id_to_count

    def __call__(self, *args, **kwargs) -> any:
        if self.task_count:
            self.task_count -= 1

        task_output = FunctionWrapper.__call__(self, *args, **kwargs)
        self.scheduled_time = time.time() + self.freq \
            if self.freq and (self.task_count is None or self.task_count > 0) \
            else None
        
        return task_output
        
    def __lt__(self, other: object):
        return self.scheduled_time < other.scheduled_time

class TaskScheduler:
    def __init__(self, request_providers: dict = {}):
        self.request_providers = request_providers

    def run_tasks(self, tasks: list, **kwargs) -> None:
        tasks = sorted(tasks)
        heapq.heapify(tasks)

        while tasks:
            task = heapq.heappop(tasks)

            while task.scheduled_time > time.time():
                time.sleep(1) # Busy waiting
            
            # Schedule requests by provider
            for request_provider_id, request_count in task.request_provider_id_to_count.items():
                if request_provider_id not in self.request_providers:
                    continue

                task.scheduled_time = self.request_providers.get(request_provider_id) \
                        .schedule_requests(request_count)

                if not task.scheduled_time <= time.time():
                    heapq.heappush(tasks, task) # Reschedule task
                    print(f"{int(time.time())}: RequestProvider <{request_provider_id}>",
                            f"reached maximum requests limit. Task <{task.task_id}>",
                            f"rescheduled to {int(task.scheduled_time)}")
                    break

            if not task.scheduled_time <= time.time():
                continue # Skip rescheduled 

            # Run requests by provider
            for request_provider_id, request_count in task.request_provider_id_to_count.items():
                if request_provider_id in self.request_providers:
                    self.request_providers.get(request_provider_id).run_requests(
                            task.task_id, request_count)

            print(f"{int(time.time())}: Running task <{task.task_id}> ... ", end='')
            task(**kwargs) # Run task
            print(f"completed at {int(time.time())}.")

            # Complete requests by provider
            for request_provider_id, request_count in task.request_provider_id_to_count.items():
                if request_provider_id in self.request_providers:
                    self.request_providers.get(request_provider_id).complete_requests(task.task_id)

            if task.scheduled_time:
                heapq.heappush(tasks, task)
                print(f"{int(time.time())}: Task <{task.task_id}>",
                    f"rescheduled to {int(task.scheduled_time)}")

if __name__ == "main":
    pass
