from multiprocessing.managers import ListProxy
from pyutils.scheduler.task import Task
from pyutils.scheduler.task import never_predicate

class ListAccumulator (Task):
    def __init__(self, target_function: callable, key: str, accumulate_to_list: ListProxy, *args: any,
        resource_usage: dict = dict(), reschedule_pred: callable = never_predicate, reschedule_freq: float = 0,
        raise_on_except: bool = True, remove_task_state_on_done: bool = False, private_mode: bool = False,
        **kwargs: any):
        """ Parameters:
            target_function (callable): The function to run on job execution.
            key (str): Unique key identifier.
            resource_usage (dict, opt): The mapping of resource_key (str): units of resource_usage (int).
            reschedule_pred (callable, opt): Predicate function accepting output of the target_function
                    to determine whether the job should be rescheduled.
            reschedule_freq (float, opt): The frequency at which to reschedule the job. This frequency
                    excludes execution time / rescheduled with respect from the start of execution.
            raise_on_except (bool, opt): Whether to raise exceptions.
            remove_task_state_on_done (bool, opt): Whether to remove the task from the task_manager on finish.
            *args, **kwargs (opt): Arguments and keyword-arguments to pass during execution.
        """
        Task.__init__(self, target_function, key, *args, resource_usage=resource_usage, reschedule_pred=reschedule_pred,
                reschedule_freq=reschedule_freq, raise_on_except=raise_on_except,
                remove_task_state_on_done=remove_task_state_on_done, private_mode=private_mode, **kwargs)

        self.accumulate_to_list = accumulate_to_list

    def update_output(self, output: any) -> None:
        self.accumulate_to_list.append(output)

if __name__ == "__main__":
    pass