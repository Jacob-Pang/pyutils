from multiprocessing.managers import ListProxy
from pyutils.scheduler.task import Task
from pyutils.scheduler.task import never_predicate

class ListAccumulator (Task):
    def __init__(self, target_function: callable, key: str, accumulate_to_list: ListProxy, *args: any,
        resource_usage: dict = dict(), reschedule_pred: callable = never_predicate, reschedule_freq: float = 0,
        retry_on_except: int = 0, raise_on_except: bool = True, retry_freq: float = 0,
        remove_task_state_on_done: bool = False, private_mode: bool = False, **kwargs: any) -> None:

        Task.__init__(
            self, target_function, key, *args,
            resource_usage=resource_usage,
            reschedule_pred=reschedule_pred,
            reschedule_freq=reschedule_freq,
            retry_on_except=retry_on_except,
            raise_on_except=raise_on_except,
            retry_freq=retry_freq,
            remove_task_state_on_done=remove_task_state_on_done,
            private_mode=private_mode,
            **kwargs
        )

        self.accumulate_to_list = accumulate_to_list

    def update_run(self, output: any, start_time: float, tasks_to_register: dict) -> None:
        self.accumulate_to_list.append(output)
        Task.update_run(self, output, start_time, tasks_to_register)

if __name__ == "__main__":
    pass