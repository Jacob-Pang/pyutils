import time

from multiprocessing.managers import SyncManager

from pyutils import generate_unique_key
from pyutils.scheduler.task import Task
from pyutils.scheduler.resource import Resource
from pyutils.scheduler.resource.resource_unit import ResourceUnit

def update_usage_limiter() -> None:
    pass

class UsageLimiter (Resource):
    def __init__(self, window: int, *resource_units: ResourceUnit, key: str = None, units: dict = dict(),
        usage: dict = dict(), usage_updates: dict = dict()) -> None:

        super().__init__(*resource_units, key=key, units=units, usage=usage)
        self.window = window
        self.usage_updates = usage_updates # {update_task_key: (resource_unit_key, usage_to_free)}

    def use(self, usage: int, task_key: str, resource_unit: ResourceUnit = None) -> None:
        if task_key in self.usage_updates.keys():
            return

        return super().use(usage, task_key, resource_unit)

    def free(self, usage: int, task_key: str, resource_unit: ResourceUnit, update_tasks: dict) -> None:
        if task_key in self.usage_updates.keys():
            resource_unit_key, usage = self.usage_updates.pop(task_key)
            resource_unit = self.units.get(resource_unit_key)

            return super().free(usage, task_key, resource_unit, update_tasks)

        update_task_key = generate_unique_key(suffix=self.key)

        update_task = Task(
            update_usage_limiter,
            key=update_task_key,
            resource_usage={self.key: 0},
            remove_task_state_on_done=True,
            visible=False,
            private=True
        )

        self.usage_updates[update_task_key] = (resource_unit.key, usage)
        update_tasks[update_task] = time.time() + self.window

    def as_shared_proxy(self, sync_manager: SyncManager):
        return UsageLimiter(
            self.window,
            key=self.key,
            units=sync_manager.dict(self.units),
            usage=sync_manager.dict(self.usage),
            usage_updates=sync_manager.dict(self.usage_updates)
        )

if __name__ == "__main__":
    pass