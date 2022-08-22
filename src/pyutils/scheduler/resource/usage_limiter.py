import time

from collections import deque
from collections.abc import Iterable
from pyutils.scheduler.resource import ResourceGate
from multiprocessing import Semaphore

class UsageLimiter (ResourceGate):
    @staticmethod
    def init_usage_limiter(units_window_list: Iterable, **attributes):
        # Returns a chained UsageLimiter object from collection of (capacity, window).
        usage_limiter = None

        for units, window in sorted(units_window_list, key=lambda capacity_window: capacity_window[1]):
            usage_limiter = UsageLimiter(units, window, child=usage_limiter, **attributes)

        return usage_limiter

    def __init__(self, units: int, window: int, gate_id: str = None, child: any = None, usage_buffer:
        ResourceGate.ResourceBuffer = None, reserve_buffer: ResourceGate.ResourceBuffer = None,
        semaphore: Semaphore = None, **attributes) -> None:

        if child: # Chained ResourceLimiter
            assert isinstance(child, UsageLimiter) and child.units <= units and \
                    child.window <= window
            
            child.parent = self
            ResourceGate.__init__(self, units, gate_id, child.usage_buffer, child.reserve_buffer,
                    child.semaphore, **attributes)
        else:
            ResourceGate.__init__(self, units, gate_id, usage_buffer, semaphore,
                    reserve_buffer, **attributes)
            
        self.window = window
        self.freed_usage_queue = deque()
        self.freed_usage = 0
        
        self.parent = None
        self.child = child

    def free(self, units: int) -> None:
        if self.child:
            return self.child.free(units)

        super().free(units)
        self.freed_usage_queue.append((time.time(), units))
        self.freed_usage += units

    def get_window_boundary(self) -> float:
        return time.time() - self.window

    def get_window_usage(self) -> int:
        # Returns the number of units used within the current window
        if self.child:
            return self.freed_usage + self.child.get_window_usage()
        
        return self.freed_usage + self.usage_buffer.units

    def get_free_units_bypass_update(self) -> int:
        return self.units - self.get_window_usage() - self.reserve_buffer.units

    def get_free_units(self) -> int:
        if not self.child:
            self.update()
            return self.get_free_units_bypass_update()
        
        child_available_units = self.child.get_free_units()
        self.update()
        return min(self.get_free_units_bypass_update(), child_available_units)

    def get_free_time_bypass_update(self, units: int, window: int) -> float:
        units = max(units - self.get_free_units_bypass_update(), 0)
        next_timestamp = 0

        for freed_timestamp, freed_usage in self.freed_usage_queue:
            if not units:
                break

            next_timestamp = freed_timestamp + window
            units = max(units - freed_usage, 0)

        if units and self.child:
            return self.child.get_free_time_bypass_update(units, window)
        
        return next_timestamp if not units else None

    def get_free_time(self, units: int) -> float:
        if not self.child:
            self.update()
            return self.get_free_time_bypass_update(units, self.window)
        
        child_available_timestamp = self.child.get_free_time(units)

        if child_available_timestamp is None:
            return None

        self.update()
        available_timestamp = self.get_free_time_bypass_update(units, self.window)

        if available_timestamp is None:
            return None

        return max(available_timestamp, child_available_timestamp)

    def update(self) -> None:
        window_boundary = self.get_window_boundary()

        while self.freed_usage_queue and self.freed_usage_queue[0][0] < window_boundary:
            freed_usage_pair = self.freed_usage_queue.popleft()
            self.freed_usage = self.freed_usage - freed_usage_pair[1]

            if self.parent: # Cascade released usage upwards
                self.parent.freed_usage_queue.append(freed_usage_pair)

    def get_repr(self, resource_id: str) -> str:
        representation = f"RSRC {resource_id[:25]:<25} GATE {f'{self.gate_id} WIND {self.window}'[:25]:<25} " + \
                f"[ UTIL : {self.get_window_usage():<4} / {self.units:<4} | BLOCKED : {self.reserve_buffer.units:<4} ]"

        if self.child:
            return f"{representation}\n{self.child.get_repr(resource_id)}"

        return representation
    
if __name__ == "__main__":
    pass