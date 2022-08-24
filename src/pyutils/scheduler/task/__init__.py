import time

from pyutils import WrappedFunction
from pyutils.scheduler.task.task_state import TaskState
from pyutils.scheduler.task.task_state import NewState, DoneState, ExceptionState

def never_predicate(output: any) -> bool:
    return False 

class Task (WrappedFunction):
    def __init__(self, target_function: callable, key: str, *args: any, resource_usage: dict = dict(),
        reschedule_pred: callable = never_predicate, reschedule_freq: float = 0, raise_on_except: bool = True,
        delete_task_on_done: bool = False, private_mode: bool = False, **kwargs: any):
        """ Parameters:
            target_function (callable): The function to run on job execution.
            key (str): Unique key identifier.
            resource_usage (dict, opt): The mapping of resource_key (str): units of resource_usage (int).
            reschedule_pred (callable, opt): Predicate function accepting output of the target_function
                    to determine whether the job should be rescheduled.
            reschedule_freq (float, opt): The frequency at which to reschedule the job. This frequency
                    excludes execution time / rescheduled with respect from the start of execution.
            raise_on_except (bool, opt): Whether to raise exceptions.
            delete_task_on_done (bool, opt): Whether to delete the task from the task_manager on finish.
            *args, **kwargs (opt): Arguments and keyword-arguments to pass during execution.
        """
        super().__init__(target_function, *args, **kwargs)
        self.key = key
        self.resource_usage = resource_usage

        self.reschedule_freq = reschedule_freq
        self.reschedule_pred = reschedule_pred
        self.raise_on_except = raise_on_except

        self.delete_task_on_done = delete_task_on_done
        self.private_mode = private_mode

    def __call__(self, *args, **kwargs) -> TaskState:
        start_time = time.time()

        try:   output = super().__call__(*args, **kwargs)
        except Exception as exception:
            if self.raise_on_except:
                raise exception
            
            return ExceptionState(self.key, time.time(), private_mode=self.private_mode, remove_state=self.delete_task_on_done)

        return NewState(self.key, max(start_time + self.reschedule_freq, time.time()), self.private_mode) \
                if self.reschedule_pred(output) else \
                DoneState(self.key, private_mode=self.private_mode, remove_state=self.delete_task_on_done)

if __name__ == "__main__":
    pass