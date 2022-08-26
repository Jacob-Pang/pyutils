import inspect
import time

_KEYS = 0

def generate_unique_key(prefix: str = "", suffix: str = "") -> str:
    global _KEYS
    _KEYS = (_KEYS + 1) % 100
    return f"{prefix}{int(time.time())}{_KEYS:02}{suffix}"

class WrappedFunction:
    @staticmethod
    def get_compat_kwargs(method: callable, **kwargs) -> dict:
        # Returns the set of kwargs that are compatible with <method>.
        if not kwargs: return kwargs

        keywords = [
            *inspect.getfullargspec(method)[0],  # args
            *inspect.getfullargspec(method)[4]   # kwonlyargs
        ]

        return {kw: arg for kw, arg in kwargs.items() if kw in keywords}

    def __init__(self, target_function: callable, *args: any, **kwargs: any):
        self.target_function = target_function
        self.args = args
        self.kwargs = WrappedFunction.get_compat_kwargs(target_function, **kwargs)

    def set_kwargs(self, kwargs: dict) -> dict:
        for kw, arg in self.kwargs.items():
            if kw in kwargs:
                continue

            kwargs[kw] = arg # override

        return WrappedFunction.get_compat_kwargs(self.target_function, **kwargs)

    def __call__(self, *args, **kwargs) -> any:
        # Uses args if passed otherwise uses args passed on __init__.
        kwargs = self.set_kwargs(kwargs)

        return self.target_function(*args, **kwargs) if args else \
            self.target_function(*self.args, **kwargs)

if __name__ == "__main__":
    pass