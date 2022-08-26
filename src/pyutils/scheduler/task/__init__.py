import time

from pyutils import WrappedFunction

# Predicates
def never_predicate(output: any) -> bool:
    return False 

def always_predicate(output: any) -> bool:
    return True

def bool_value_predicate(output: any) -> bool:
    return bool(output)

class Task (WrappedFunction):
    def __init__(self, target_function: callable, key: str, *args: any, resource_usage: dict = dict(),
        reschedule_pred: callable = never_predicate, reschedule_freq: float = 0, retry_on_except: int = 0,
        raise_on_except: bool = True, retry_freq: float = 0, remove_task_state_on_done: bool = False,
        private_mode: bool = False, **kwargs: any) -> None:
        """ Parameters:
            target_function (callable): The function to run on job execution.
            key (str): Unique key identifier.
            resource_usage (dict, opt): The mapping of resource_key (str): units of resource_usage (int).
            reschedule_pred (callable, opt): Predicate function accepting output of the target_function
                    to determine whether the task should be rescheduled.
            reschedule_freq (float, opt): The frequency at which to reschedule the task. This frequency
                    excludes execution time / rescheduled with respect from the start of execution.
            retry_on_except (int, opt): The number of times to retry on exception.
            raise_on_except (bool, opt): To raise exceptions post-retry attempts.
            remove_task_state_on_done (bool, opt): Whether to remove the task from the task_manager on
                    completion (no further rescheduling of self).
            retry_freq (float, opt): The frequency at which to retry the task, determined from the point
                    of exception rather than the start_time of execution.
            *args, **kwargs (opt): Arguments and keyword-arguments to pass during execution.
        """
        super().__init__(target_function, *args, **kwargs)
        self.key = key
        self.resource_usage = resource_usage

        self.reschedule_freq = reschedule_freq
        self.reschedule_pred = reschedule_pred
        self.retry_on_except = retry_on_except
        self.raise_on_except = raise_on_except
        self.retry_freq = retry_freq

        self.remove_task_state_on_done = remove_task_state_on_done
        self.private_mode = private_mode

        self.run_count = 0
        self.retry_attempts = 0

    def update_run(self, output: any, start_time: float, tasks_to_register: dict) -> None:
        self.run_count += 1
        self.retry_attempts = 0

        if self.reschedule_pred(output):
            tasks_to_register[self] = start_time + self.reschedule_freq

    def update_exception(self, exception: Exception, tasks_to_register: dict) -> None:
        if self.retry_attempts >= self.retry_on_except:
            if self.raise_on_except:
                raise exception
        
            return # Ignore exception but no retry.
        
        self.retry_attempts += 1
        tasks_to_register[self] = time.time() + self.retry_freq

    def __call__(self, *args, tasks_to_register: dict = dict(), **kwargs) -> bool:
        # Returns task execution state
        start_time = time.time()

        try:
            output = super().__call__(*args, **kwargs, tasks_to_register=tasks_to_register)
            self.update_run(output, start_time, tasks_to_register)
            return True
        except Exception as exception:
            self.update_exception(exception, tasks_to_register)
            return False

if __name__ == "__main__":
    pass