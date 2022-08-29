from pyutils import generate_unique_key

class ResourceUnit:
    def __init__(self, capacity: int, key: str = None, **attrs: any) -> None:
        if not key:
            key = generate_unique_key(prefix="RU_")

        self.key = key
        self.capacity = capacity

        for attr_name, attr_value in attrs.items():
            setattr(self, attr_name, attr_value)

    def get_state_repr(self, resource_key: str, usage: int) -> str:
        return f"RESOURCE | {resource_key:<20} : {self.key:<20} [ {usage:>5} / {self.capacity:>8} ]"

if __name__ == "__main__":
    pass