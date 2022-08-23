import colorama

from pyutils import StateNamespace
from pyutils import _STATE

colorama.init()

_STATE.set_attr(
    temporary_lines=0
)

def flush_temporary_lines(state: StateNamespace = _STATE) -> None:
    if not hasattr(state, "temporary_lines"):
        state.set_attr("temporary_lines", 0)
    
    for _ in range(state.temporary_lines):
        # move up cursor and delete whole line
        print("\x1b[1A\x1b[2K", end='\r')
    
    state.temporary_lines = 0

def temporary_print(message: str, state: StateNamespace = _STATE) -> None:
    flush_temporary_lines() # Remove existing temporary lines
    state.temporary_lines = message.count('\n') + 1
    print(message)

if __name__ == "__main__":
    pass