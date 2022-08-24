from multiprocessing.managers import SyncManager
from pyutils.scheduler.resource.resource_unit import ResourceUnit

class Resource:
    def __init__(self, key: str, *resource_units: ResourceUnit, units: dict = dict(), usage: dict = dict()) -> None:
        self.key = key
        self.units = units
        self.usage = usage

        for resource_unit in resource_units:
            self.register_unit(resource_unit)

    def register_unit(self, resource_unit: ResourceUnit) -> None:
        self.units[resource_unit.key] = resource_unit
        self.usage[resource_unit.key] = 0

    def get_free_unit(self, usage: int) -> ResourceUnit:
        """ Returns any resource_unit with sufficient spare capacity.

        Parameters:
            usage (int): The usage capacity requested.
            usage_state (DictProxy): Mapping of unit_key: usage.
        """
        for unit_key, resource_unit in self.units.items():
            if self.usage.get(unit_key) + usage <= resource_unit.capacity:
                return resource_unit

        return None

    def use(self, usage: int, resource_unit: ResourceUnit = None) -> None:
        if not resource_unit:
            resource_unit = self.get_free_unit(usage)

        self.usage[resource_unit.key] = (self.usage[resource_unit.key] + usage)

    def free(self, usage: int, resource_unit: ResourceUnit, update_tasks: dict) -> None:
        self.usage[resource_unit.key] = (self.usage[resource_unit.key] - usage)

    def __str__(self) -> str:
        return "\n".join([
            self.units.get(unit_key).get_state_repr(self.key, usage)
            for unit_key, usage in self.usage.items()
        ])

class ResourceProxy (Resource):
    def __init__(self, resource: Resource, sync_manager: SyncManager) -> None:
        Resource.__init__(self, resource.key, units=sync_manager.dict(resource.units),
                usage=sync_manager.dict(resource.usage))

if __name__ == "__main__":
    pass