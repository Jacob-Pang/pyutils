from uuid import uuid4

class ResourceBase:
    def __init__(self, key: str = None):
        if not key:
            key = uuid4()

        self.key = key

    def use(self, units: int) -> str:
        raise NotImplementedError()

    def free(self, key: str, units: int) -> None:
        raise NotImplementedError()

    def has_free_capacity(self, units: int) -> bool:
        raise NotImplementedError()

    def update(self) -> bool:
        # Returns whether any changes were made to the state.
        return False

    def get_timeout_to_update(self) -> float:
        # Returns the timeout to wait before updating the resource.
        # Returns None where there is no updates required.
        return None

    def __repr__(self) -> str:
        raise NotImplementedError()

if __name__ == "__main__":
    pass
