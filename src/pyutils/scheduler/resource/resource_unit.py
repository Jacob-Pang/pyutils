class ResourceUnit:
    def __init__(self, key: str, capacity: int, **attrs: any) -> None:
        self.key = key
        self.capacity = capacity

        for attr_name, attr_value in attrs.items():
            setattr(self, attr_name, attr_value)

    def get_state_repr(self, resource_key: str, usage: int) -> str:
        return f"RESOURCE | {resource_key:<20} : {self.key:<20} [ {usage:>5} / {self.capacity:>8} ]"

if __name__ == "__main__":
    pass