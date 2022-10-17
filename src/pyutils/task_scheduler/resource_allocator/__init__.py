from abc import ABC
from abc import abstractmethod
from collections import deque
from uuid import uuid4
from sorted_dict import SortedDictBase, SortedDict
from sorted_dict.priority_dict import PriorityDict
from task_scheduler.resource import ResourceBase

class ResourceAllocatorBase (ABC):
    """ Resource allocator and aliased proxy.
    Phases of usage requests (task_key, units):
        1. new      : unprocessed request
        2. ready    : resources allocated and pending task execution
        3. using    : resources consumed and task in execution
        4. waiting  : waiting for allocation or unable to allocate resources

    Allocator states:
        1. ready    : no outstanding waiting requests (post update)
        2. waiting  : allocation failure with outstanding waiting requests
    """
    def __init__(self, alias: str = None):
        self.alias = alias if alias else uuid4()

        self._resources = dict() # {resource_key: resource}
        self._resources_capacity = SortedDict(SortedDictBase.min_comparator) # {resource_key: capacity}
        self._ready_allocation = dict() # {task_key: resource_key}
        self._ready_queue = PriorityDict(SortedDictBase.max_comparator) # {task_key: units}
        self._waiting_queue = deque() # [(task_key, units) ...]
        self._ready_usage = 0 # usage units for requests in ready_queue
        self._waiting_requests = 0 # outstanding requests post-update
    
    # Accessors
    def get_time_to_update(self) -> float:
        # Returns the shortest time to the next update.
        time_to_update = 5

        for resource in self._resources.values():
            time_to_update = min(time_to_update, resource.get_time_to_update())
        
        return time_to_update

    def get_allocated_resource(self, task_key: str) -> (str | None):
        # Returns the allocated resource_key if allocated otherwise returns None
        if task_key in self._ready_allocation:
            return self._ready_allocation.get(task_key)
        
        return None
    
    # Mutators
    def register_resource(self, resource: ResourceBase) -> None:
        self._resources[resource.key] = resource
        self._resources_capacity[resource.key] = resource.get_free_capacity()

    def register_request(self, task_key: str, units: int) -> None:
        self._waiting_queue.append((task_key, units))
                
    def use(self, task_key: str) -> None:
        # Use resources based on prior allocation from waiting queue and dequeues the request.
        units = self._ready_queue.pop(task_key)
        resource_key = self._ready_allocation.pop(task_key)

        self._ready_usage -= units
        self._resources[resource_key].use(units)
        self._resources_capacity[resource_key] -= units
    
    def free(self, resource_key: str, units: int) -> None:
        # Does not update tracked resource capacities
        self._resources[resource_key].free(units)
        
    @abstractmethod
    def dequeue_and_allocate(self, net_capacity: int, max_capacity: int) -> None:
        """ Attempt to dequeue waiting requests and allocate resources - transiting to ready states:
        Such that there exists a configuration (the ready_allocation) such that the resources can accomodate
        the requests in the ready_queue (invariant). 
        """
        raise NotImplementedError()

    def update(self) -> None:
        max_capacity, net_capacity = 0, 0
        capacity_change = False

        for resource in self._resources.values():
            resource.update()

            capacity = resource.get_free_capacity()
            max_capacity = max(max_capacity, capacity)
            net_capacity += capacity

            if capacity > self._resources_capacity[resource.key]:
                capacity_change = True

            self._resources_capacity[resource.key] = capacity

        if (self._waiting_requests and capacity_change) or \
            (not self._waiting_requests and len(self._waiting_queue)):
            # waiting state and capacity freed or
            # ready state with new requests registered
            self.dequeue_and_allocate(net_capacity, max_capacity)

        # Update outstanding waiting requests
        self._waiting_requests = len(self._waiting_queue)

class ResourceAllocator (ResourceAllocatorBase):
    def allocate_resources(self) -> tuple[dict, int, int]:
        """ Attempt to allocate resources to requests in the ready_queue.
        Returns the allocation, number of unallocated requests and max remainder capacity.
        """
        _resources_capacity = self._resources_capacity.copy()
        _ready_queue = self._ready_queue.copy()
        _ready_allocation = dict()

        while _ready_queue:
            task_key, units = _ready_queue.front()

            for resource_key, capacity in _resources_capacity.items():
                if capacity >= units: # Sufficient capacity
                    _ready_allocation[task_key] = resource_key
                    _resources_capacity[resource_key] = (capacity - units)
                    _ready_queue.popitem()
                    break

            if not task_key in _ready_allocation:
                break # Cannot allocate any more requests
    
        unallocated_requests = 0

        for _, units in _ready_queue._kv_container: # Order does not matter
            unallocated_requests += units

        _, max_capacity = _resources_capacity._kv_container[-1]
        return _ready_allocation, unallocated_requests, max_capacity

    def dequeue_and_allocate(self, net_capacity: int, max_capacity: int) -> None:
        transit_stack = []
        _net_capacity = net_capacity - self._ready_usage

        def dequeue_next_waiting_request():
            task_key, units = self._waiting_queue.popleft()
            self._ready_queue[task_key] = units
            self._ready_usage += units

        # Dequeue max requests
        while self._waiting_queue:
            task_key, units = self._waiting_queue[0]

            if units > max_capacity or units > _net_capacity:
                break # Unable to accomodate next request

            dequeue_next_waiting_request()
            transit_stack.append(task_key)
            _net_capacity -= units

        self._ready_allocation.clear() # Remove prior allocation

        while True:
            # By the property of the invariant
            # There must be a solution when transit_stack is empty
            _ready_allocation, unallocated_units, max_capacity = self.allocate_resources()

            if not unallocated_units: # Allocation success
                self._ready_allocation = _ready_allocation

                if not self._waiting_queue: # No outstanding waiting requests
                    return

                task_key, units = self._waiting_queue[0]

                if units > max_capacity or self._ready_usage + units > net_capacity:
                    return # Cannot allocate the next waiting request

                dequeue_next_waiting_request()
                continue

            if self._ready_allocation:
                return # Optimal solution found in previous iteration

            # Requeue unallocated requests
            while unallocated_units > 0:
                task_key = transit_stack.pop()
                units = self._ready_queue.pop(task_key)
                unallocated_units = unallocated_units - units
                self._ready_usage -= units
                self._waiting_queue.appendleft((task_key, units))

if __name__ == "__main__":
    pass