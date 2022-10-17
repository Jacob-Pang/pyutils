from multiprocessing import Queue
from multiprocessing.managers import SyncManager
from task_scheduler.resource import ResourceBase
from task_scheduler.resource_allocator import ResourceAllocatorBase, ResourceAllocator

class ResourceManagerProxy:
    def __init__(self, _freed_queue: Queue) -> None:
        self._freed_queue = _freed_queue
    
    def free_resources(self, resource_usage: dict, allocated_resources: dict) -> None:
        for alias, units in resource_usage.items():
            self._freed_queue.put((alias, allocated_resources.get(alias), units))

class ResourceManager (ResourceManagerProxy):
    def __init__(self, sync_manager: SyncManager) -> None:
        self._resource_allocators = dict()
        ResourceManagerProxy.__init__(self, sync_manager.Queue())

    # Accessors
    def get_time_to_update(self) -> float:
        time_to_update = 5

        for resource_allocator in self._resource_allocators.values():
            time_to_update = min(time_to_update, resource_allocator.get_time_to_update())
        
        return time_to_update

    def get_allocated_resources(self, task_key: str, resource_usage: dict) -> (dict | None):
        # Returns the allocated resource_keys if successfully allocated otherwise None.
        allocated_resources = dict()
        
        for alias in resource_usage:
            allocated_key = self._resource_allocators[alias].get_allocated_resource(task_key)

            if not allocated_key:
                return None

            allocated_resources[alias] = allocated_key

        return allocated_resources

    # Mutators
    def register_allocator(self, resource_allocator: ResourceAllocatorBase) -> None:
        self._resource_allocators[resource_allocator.alias] = resource_allocator

    def register_resource(self, resource: ResourceBase, alias: str = None) -> None:
        if alias in self._resource_allocators:
            return self._resource_allocators[alias].register_resource(resource)

        # Constructs default resource allocator
        resource_allocator = ResourceAllocator(alias)
        resource_allocator.register_resource(resource)
        self.register_allocator(resource_allocator)

    def register_request(self, task_key: str, resource_usage: dict) -> None:
        # Registers the request but does not update the resource allocators.
        for alias, units in resource_usage.items():
            self._resource_allocators[alias].register_request(task_key, units)

    def use_resources(self, task_key: str, allocated_resources: dict) -> None:
        for alias in allocated_resources:
            self._resource_allocators[alias].use(task_key)

    def update(self) -> None:
        while not self._freed_queue.empty():
            alias, resource_key, units = self._freed_queue.get()
            self._resource_allocators[alias].free(resource_key, units)

        for resource_allocator in self._resource_allocators.values():
            resource_allocator.update()

    def get_proxy(self) -> ResourceManagerProxy:
        return ResourceManagerProxy(self._freed_queue)

if __name__ == "__main__":
    pass