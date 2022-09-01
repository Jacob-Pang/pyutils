import time

from pyutils.task_scheduler.resource import Resource

class RateLimit (Resource):
    @staticmethod
    def init_resource(window_capacity_pairs: list, key: str = None):
        rate_limit = None

        for window, capacity in sorted(window_capacity_pairs):
            rate_limit = RateLimit(window, key, capacity, rate_limit)
        
        return rate_limit

    def __init__(self, window: float, key: str = None, capacity: int = 1, child: Resource = None) -> None:
        Resource.__init__(self, key, capacity)

        if child: # Assert that child RateLimit node must have smaller specifications.
            assert child.window < window and child.capacity < capacity
            child.parent = self

        self.window = window
        self.update_queue = []
        self.child = child
        self.parent = None

    def use(self, units: int) -> str:
        if self.child:
            return self.child.use(units)
        
        # Executed only at the deepest RateLimit node.
        Resource.use(self, units)

    def free(self, key: str, units: int) -> None:
        if self.child:
            return self.child.free(key, units)

        # Executed only at the deepest RateLimit node.
        self.update_queue.append((time.time(), units))
    
    def get_usage(self) -> int:
        if not self.child:
            return self.usage

        return self.child.get_usage() + self.usage

    def has_free_capacity(self, units: int) -> bool:
        if self.child and not self.child.has_free_capacity(units):
            return False

        return self.capacity - self.get_usage() >= units

    def update(self) -> bool:
        state_change = False

        if self.child:
            state_change = self.child.update()

        while self.update_queue:
            if self.update_queue[0][0] + self.window > time.time():
                break

            timestamp, units = self.update_queue.pop(0)
            self.usage -= units

            if self.parent: # Propagate usage upwards
                self.parent.update_queue.append((timestamp, units))
                self.parent.usage += units
        
            state_change = True

        return state_change

    def get_timeout_to_update(self) -> float:
        timeout = self.update_queue[0][0] + self.window - time.time() \
                if self.update_queue else None

        if not self.child:
            return timeout

        child_timeout = self.child.get_timeout_to_update()

        if not child_timeout:
            return timeout

        if not timeout:
            return child_timeout

        return min(timeout, child_timeout)

    def __repr__(self) -> str:
        resource_repr = f"LIMIT {str(self.key):<36} [ {self.get_usage():<4} / {self.capacity:<4} | {self.window:<4} ]"

        if self.child:
            return f"{self.child}\n{resource_repr}"

        return resource_repr

if __name__ == "__main__":
    pass