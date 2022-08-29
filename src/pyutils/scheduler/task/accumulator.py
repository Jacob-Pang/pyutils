from pyutils.scheduler.task import Task
from pyutils.scheduler.task.repeat_predicate import RepeatPredicate, CounterPredicate

class ListAccumulator (Task):
    def __init__(self, target_function: callable, accumulate_to_list: list, *args: any, key: str = None,
        resource_usage: dict = dict(), repeat_pred: RepeatPredicate = CounterPredicate, repeat_freq: float = 0,
        retry_on_except: int = 0, raise_on_except: bool = True, retry_freq: float = 0, visible: bool = True,
        private: bool = False, remove_task_state_on_done: bool = False, **kwargs: any) -> None:

        Task.__init__(
            self, target_function, *args,
            key=key,
            resource_usage=resource_usage,
            repeat_pred=repeat_pred,
            reschedule_freq=repeat_freq,
            retry_on_except=retry_on_except,
            raise_on_except=raise_on_except,
            retry_freq=retry_freq,
            visible=visible,
            private=private,
            remove_task_state_on_done=remove_task_state_on_done,
            **kwargs
        )

        self.accumulate_to_list = accumulate_to_list

    def update_run(self, output: any, start_time: float, tasks_to_register: dict) -> None:
        self.accumulate_to_list.append(output)
        Task.update_run(self, output, start_time, tasks_to_register)

    def __call__(self, *args, tasks_to_register: dict = dict(), **kwargs) -> bool:
        return super().__call__(
            *args, **kwargs,
            tasks_to_register=tasks_to_register,
            accumulate_to_list=self.accumulate_to_list
        )

if __name__ == "__main__":
    pass