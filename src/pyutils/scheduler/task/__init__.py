import time

from pyutils import generate_unique_key
from pyutils.wrapper import WrappedFunction
from pyutils.scheduler.task.repeat_predicate import RepeatPredicate, CounterPredicate

class Task (WrappedFunction):
    def __init__(self, target_function: callable, *args: any, key: str = None, resource_usage: dict = dict(),
        repeat_pred: RepeatPredicate = CounterPredicate(1), repeat_freq: float = 0, retry_on_except: int = 0,
        raise_on_except: bool = True, retry_freq: float = 0, visible: bool = True, private: bool = False,
        remove_task_state_on_done: bool = False, **kwargs: any) -> None:
        """ Parameters:
            target_function (callable): The function to run on job execution.
            key (str): Unique identifier.
            resource_usage (dict, opt): The mapping of resource_key (str): units of resource_usage (int).
            repeat_pred (RepeatPredicate, opt): Predicate function accepting output of the target_function to determine
                    whether the task should be repeated.
            repeat_freq (float, opt): The frequency at which to repeat the task. This frequency excludes execution time
                    / repeated with respect from the start of execution.
            retry_on_except (int, opt): The number of times to retry on exception.
            raise_on_except (bool, opt): To raise exceptions post-retry attempts.
            remove_task_state_on_done (bool, opt): Whether to remove the task from the task_manager on
                    completion (no further rescheduling of self).
            retry_freq (float, opt): The frequency at which to retry the task, determined from the point
                    of exception rather than the start_time of execution.
            *args, **kwargs (opt): Arguments and keyword-arguments to pass during execution.
        """
        super().__init__(target_function, *args, **kwargs)
        
        if not key:
            key = generate_unique_key(prefix="T_")

        self.key = key
        self.resource_usage = resource_usage
        self.repeat_freq = repeat_freq
        self.repeat_pred = repeat_pred
        self.retry_on_except = retry_on_except
        self.raise_on_except = raise_on_except
        self.retry_freq = retry_freq
        self.visible = visible
        self.private = private
        self.remove_task_state_on_done = remove_task_state_on_done
        self.run_count = 0
        self.retry_attempts = 0

    def update_run(self, output: any, start_time: float, tasks_to_register: dict) -> None:
        self.run_count += 1
        self.retry_attempts = 0

        if self.repeat_pred(output):
            tasks_to_register[self] = start_time + self.repeat_freq

    def update_exception(self, exception: Exception, tasks_to_register: dict) -> None:
        if self.retry_attempts >= self.retry_on_except:
            if self.raise_on_except:
                raise exception
        
            return # Ignore exception but no retry.
        
        self.retry_attempts += 1
        tasks_to_register[self] = time.time() + self.retry_freq

    def __call__(self, *args, assigned_resource_units: dict = dict(), tasks_to_register: dict = dict(), **kwargs) -> bool:
        # Returns task execution state
        start_time = time.time()

        try:
            output = super().__call__(
                *args, **kwargs,
                assigned_resource_units=assigned_resource_units,
                tasks_to_register=tasks_to_register
            )

            self.update_run(output, start_time, tasks_to_register)
            return True

        except Exception as exception:
            self.update_exception(exception, tasks_to_register)

            return False

if __name__ == "__main__":
    pass