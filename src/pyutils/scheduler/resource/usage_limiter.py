import time

from multiprocessing.managers import SyncManager
from pyutils import generate_unique_key
from pyutils.scheduler.task import Task
from pyutils.scheduler.resource import Resource
from pyutils.scheduler.resource.resource_unit import ResourceUnit

def empty_function() -> None:
    pass

class UsageLimiter (Resource):
    def __init__(self, key: str, window: int, *resource_units: ResourceUnit, units: dict = dict(),
        usage: dict = dict(), usage_updates: dict = dict()) -> None:

        super().__init__(key, *resource_units, units=units, usage=usage)
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

        update_task_key = generate_unique_key()
        update_task = Task(empty_function, key=update_task_key, resource_usage={self.key: 0},
                delete_task_on_done=True, private_mode=True)

        self.usage_updates[update_task_key] = (resource_unit.key, usage)
        update_tasks[update_task] = time.time() + self.window

    def create_proxy(self, sync_manager: SyncManager):
        return UsageLimiterProxy(self, sync_manager)

class UsageLimiterProxy (UsageLimiter):
    def __init__(self, resource: UsageLimiter, sync_manager: SyncManager) -> None:
        UsageLimiter.__init__(self, resource.key, resource.window, units=sync_manager.dict(resource.units),
                usage=sync_manager.dict(resource.usage), usage_updates=sync_manager.dict(resource.usage_updates))

if __name__ == "__main__":
    pass