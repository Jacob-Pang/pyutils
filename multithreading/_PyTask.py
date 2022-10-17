from Future import Future
from pyutils.wrapper import WrappedFunction

class PyTask (WrappedFunction):
    def __init__(self, target_function: callable, *args: any, **kwargs: any):
        super().__init__(target_function, *args, **kwargs)
        self._future = Future()

    def get_future(self) -> Future:
        return self._future
        
    def __call__(self) -> None:
        output = super().__call__()
        self._future.set(output)

if __name__ == "__main__":
    pass