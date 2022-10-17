from abc import ABC
from abc import abstractmethod
from uuid import uuid4

class ResourceBase (ABC):
    def __init__(self, key: str = None):
        self.key = key if key else uuid4()

    # Accessors
    def get_time_to_update(self) -> float:
        # Returns the time in seconds to next update
        return 5

    @abstractmethod
    def get_free_capacity(self) -> int:
        pass

    def has_free_capacity(self, units: int) -> bool:
        return self.get_free_capacity() >= units

    # Mutators
    def update(self) -> bool:
        # Performs updates and returns whether a state change has occured.
        return False

    @abstractmethod
    def use(self, units: int) -> None:
        pass
    
    @abstractmethod
    def free(self, units: int) -> None:
        pass

class Resource (ResourceBase):
    def __init__(self, capacity: int = 1, key: str = None):
        ResourceBase.__init__(self, key)
        self._capacity = capacity
        self._usage = 0

    def get_free_capacity(self) -> int:
        return self._capacity - self._usage

    # Mutators
    def use(self, units: int) -> None:
        assert self.has_free_capacity(units)
        self._usage += units
    
    def free(self, units: int) -> None:
        assert self._usage >= units
        self._usage -= units

if __name__ == "__main__":
    pass