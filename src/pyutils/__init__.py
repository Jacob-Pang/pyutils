import time

_KEYS = 0

def generate_unique_key(prefix: str = "", suffix: str = "") -> str:
    global _KEYS
    _KEYS = (_KEYS + 1) % 100
    return f"{prefix}{int(time.time())}{_KEYS:02}{suffix}"

if __name__ == "__main__":
    pass