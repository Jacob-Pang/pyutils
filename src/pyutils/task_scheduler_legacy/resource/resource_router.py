from pyutils.task_scheduler_legacy.resource.base import ResourceBase

class ResourceRouter (ResourceBase):
    def __init__(self, key: str = None):
        ResourceBase.__init__(self, key)
        self._resources = dict()
    
    def add_resources(self, *resources: ResourceBase):
        for resource in resources:
            self._resources[resource.key] = resource

    def use(self, units: int) -> str:
        for resource_key, resource in self._resources.items():
            if resource.has_free_capacity(units):
                resource.use(units)

                return resource_key

        return None

    def free(self, key: str, units: int) -> None:
        self._resources.get(key).free(key, units)

    def has_free_capacity(self, units: int) -> None:
        for resource in self._resources.values():
            if resource.has_free_capacity(units):
                return True

        return False

    def update(self) -> None:
        state_change = False

        for resource in self._resources.values():
            if resource.update():
                state_change = True

        return state_change

    def get_timeout_to_update(self) -> float:
        timeout = None

        for resource in self._resources.values():
            _timeout = resource.get_timeout_to_update()

            if not _timeout:
                continue

            if not timeout or _timeout < timeout:
                timeout = _timeout

        return timeout

    def __repr__(self) -> str:
        return f"ALIAS {self.key}" + " {\n" + \
            "\n".join([
                str(resource) for resource in self._resources.values()
            ]) + "\n}"

if __name__ == "__main__":
    pass