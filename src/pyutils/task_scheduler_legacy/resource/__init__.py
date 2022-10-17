from pyutils.task_scheduler_legacy.resource.base import ResourceBase

class Resource (ResourceBase):
    def __init__(self, key: str = None, capacity: int = 1) -> None:
        ResourceBase.__init__(self, key)
        
        self.capacity = capacity
        self._usage = 0
    
    def use(self, units: int) -> str:
        if not self.has_free_capacity(units):
            return None

        self._usage += units
        return self.key

    def free(self, key: str, units: int) -> None:
        self._usage -= units

    def has_free_capacity(self, units: int) -> None:
        return self.capacity - self._usage >= units

    def __repr__(self) -> str:
        return f"RESRC {str(self.key):<36} [ {self._usage:<4} / {self.capacity:<4} ]"

if __name__ == "__main__":
    pass