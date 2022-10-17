from multiprocessing import Event

class Future:
    def __init__(self):
        self.output = None
        self.reached = Event()

    def set(self, output: any) -> None:
        self.output = output
        self.reached.set()

    def get(self, timeout: int = None) -> any:
        self.reached.wait(timeout=timeout)

        return self.output

if __name__ == "__main__":
    pass