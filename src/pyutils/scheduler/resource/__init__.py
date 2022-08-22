from multiprocessing import Semaphore
from pyutils import generate_unique_id

class ResourceGate:
    class ResourceBuffer:
        # Records usage by busy processes
        def __init__(self) -> None:
            self.units = 0

    def __init__(self, units: int = 1, gate_id: str = None, usage_buffer: ResourceBuffer = None,
        reserve_buffer: ResourceBuffer = None, semaphore: Semaphore = None, **attributes) -> None:
        """ Represents a gate controlling resource utilization.

        Parameters:
            units (int, opt): The number of resource units allocated at the gate.
            gate_id (str, opt): ID assigned to the gate.
            attributes (kwargs, opt): Attributes to assign to the gate.
        """
        if not gate_id:         gate_id = generate_unique_id()
        if not usage_buffer:    usage_buffer = ResourceGate.ResourceBuffer()
        if not reserve_buffer:  reserve_buffer = ResourceGate.ResourceBuffer()
        if not semaphore:       semaphore = Semaphore(1)

        self.units = units
        self.gate_id = gate_id
        self.usage_buffer = usage_buffer
        self.reserve_buffer = reserve_buffer
        self.semaphore = semaphore

        for attr_name, attr_value in attributes.items():
            setattr(self, attr_name, attr_value)

    def __hash__(self) -> int:
        return self.gate_id.__hash__()

    def acquire(self) -> None:
        self.semaphore.acquire()
    
    def release(self) -> None:
        self.semaphore.release()

    def reserve(self, units: int) -> None:
        self.reserve_buffer.units += units

    def use(self, units: int) -> None:
        self.reserve_buffer.units -= units
        self.usage_buffer.units += units

    def free(self, units: int) -> None:
        self.usage_buffer.units -= units
    
    def get_free_units(self) -> int:
        # Returns the number of units available for reserving usage
        return self.units - self.usage_buffer.units - self.reserve_buffer.units

    def get_free_time(self, units: int) -> float:
        return 0 if units <= self.get_free_units() else None

    def get_repr(self, resource_id: str) -> str:
        return f"Resource {resource_id[:25]:<25} Gate {self.gate_id[:25]:<25} " + \
            f"[ Usage : {self.usage_buffer.units:<4} / {self.units:<4} | " + \
            f"Scheduled : {self.reserve_buffer.units:<4} ]"

class Resource:
    def __init__(self, resource_id: str = None):
        self.resource_id = resource_id if resource_id else generate_unique_id()
        self.gates = set()

    def __hash__(self) -> int:
        return self.resource_id.__hash__()

    def acquire_free_gate_and_time(self, units: int) -> tuple:
        # Returns the time-gate (acquired) pair for the nearest free time.
        # Returns None-None if there are no gates available indefinitely.
        free_timestamp, acquired_gate = None, None

        for gate in self.gates:
            gate.acquire()
            _free_timestamp = gate.get_free_time(units)

            if _free_timestamp is None or (acquired_gate is not None and _free_timestamp > free_timestamp):
                gate.release()
                continue

            if acquired_gate: # Free previous acquired gate.
                acquired_gate.release()
            
            free_timestamp, acquired_gate = _free_timestamp, gate

        return free_timestamp, acquired_gate

    def add_gate(self, gate: ResourceGate) -> None:
        self.gates.add(gate)

    def __repr__(self) -> str:
        return "\n".join([ gate.get_repr(self.resource_id) for gate in self.gates ])

if __name__ == "__main__":
    pass