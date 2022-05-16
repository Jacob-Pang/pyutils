import sys

class RedirectIOStream:
    def __init__(self, stdout_dest = sys.stdout, stderr_dest = sys.stderr,
        stdin_dest = sys.stdin):

        self.stdout_dest = stdout_dest
        self.stderr_dest = stderr_dest
        self.stdin_dest  = stdin_dest

    def __enter__(self) -> None:
        self.origin_stdout = sys.stdout
        self.origin_stderr = sys.stderr
        self.origin_stdin  = sys.stdin

        sys.stdout = self.stdout_dest
        sys.stderr = self.stderr_dest
        sys.stdin  = self.stdin_dest

    def __exit__(self, *args, **kwargs) -> None:
        sys.stdout = self.origin_stdout
        sys.stderr = self.origin_stderr
        sys.stdin  = self.origin_stdin

class FunctionWrapper:
    def __init__(self, function: callable, **default_kwargs):
        self.wrapped_function = function
        self.default_kwargs = default_kwargs

    def updated_kwargs(self, **kwargs) -> dict:
        for kw, arg in self.default_kwargs.items():
            if kw in kwargs:
                continue

            kwargs[kw] = arg

        return kwargs

    def __call__(self, *args, **kwargs) -> any:
        return self.wrapped_function(*args, **self.updated_kwargs(**kwargs))

if __name__ == "__main__":
    pass
