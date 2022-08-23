import time
import types

class StateNamespace (types.SimpleNamespace):
    def set_attr(self, attr_name: str, attr: any, override_attr: bool = False) -> None:
        if hasattr(self, attr_name) and not override_attr:
            return
        
        setattr(self, attr_name, attr)

_STATE = StateNamespace(id_counter=0)

def generate_unique_id(state: StateNamespace = _STATE) -> str:
    state.id_counter = (state.id_counter + 1) % 100

    return f"{int(time.time())}_{state.id_counter}"

if __name__ == "__main__":
    pass