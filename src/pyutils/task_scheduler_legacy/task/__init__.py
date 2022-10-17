from multiprocessing import Event
from multiprocessing.managers import Namespace

from pyutils.wrapper import WrappedFunction
from pyutils.task_scheduler_legacy.task.base import TaskBase
from pyutils.task_scheduler_legacy.task.task_state import TaskState
from pyutils.task_scheduler_legacy.task.task_manager import TaskManagerProxy
from pyutils.task_scheduler_legacy.resource.resource_manager import ResourceManagerProxy

class Task (TaskBase, WrappedFunction):
    def __init__(self, target_function: callable, *args: any, name: str = None, start_time: float = None,
        resource_usage: dict = None, runs: int = 1, repeat_freq: float = 0, retry_on_except: int = 0,
        raise_on_except: bool = True, remove_on_done: bool = True, **kwargs: any):
        """
        Parameters:
        :target_function (callable): The function to run.
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
        :args, kwargs: Arguments and keyword-arguments to pass to the function.
        """
        WrappedFunction.__init__(self, target_function, *args, **kwargs)
        TaskBase.__init__(self, name, start_time, resource_usage, runs, repeat_freq, retry_on_except,
                raise_on_except, remove_on_done)
    
    def update_end_of_run(self, output: any, task_manager_proxy: TaskManagerProxy) -> None:
        if self._run_count == self.runs:
            task_manager_proxy.update_end_of_task(self, output)

            if self._remove_on_done:
                task_manager_proxy.remove_task_state(self.key)
            
            return
        
        self.start_time += self.repeat_freq
        task_manager_proxy.push_task(self)

    def __call__(self, allocated_keys: dict, update_event: Event, resource_manager_proxy: ResourceManagerProxy,
        task_manager_proxy: TaskManagerProxy, shared_namespace: Namespace) -> any:
        """
        """
        task_manager_proxy.update_task_state(self, TaskState.RUNNING_STATE)
        update_event.set()

        while self._retry_count <= self._retry_on_except:
            try:
                output = WrappedFunction.__call__(
                    self,
                    allocated_keys=allocated_keys,
                    task_manager_proxy=task_manager_proxy,
                    shared_namespace=shared_namespace
                )
    
                self._retry_count = 0
                self._run_count += 1

                resource_manager_proxy.update_end_of_usage(allocated_keys, self.resource_usage)
                task_manager_proxy.update_task_state(self, TaskState.DONE_STATE)
                self.update_end_of_run(output, task_manager_proxy)
                update_event.set()

                return output
            except Exception as exception:
                self._retry_count += 1

                if self._retry_count > self._retry_on_except and self._raise_on_except:
                    raise exception

        resource_manager_proxy.update_end_of_usage(allocated_keys, self.resource_usage)
        task_manager_proxy.update_task_state(self, TaskState.EXCEPT_STATE)
        task_manager_proxy.update_end_of_task(self, None)
        update_event.set()
        
        return None

if __name__ == "__main__":
    pass