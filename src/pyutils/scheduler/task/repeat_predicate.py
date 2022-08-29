
class RepeatPredicate:
    def __call__(self, task_output: any) -> bool:
        return bool(task_output)

class CounterPredicate:
    def __init__(self, count: int = 1) -> bool:
        self.count = count

    def __call__(self, task_output: any) -> bool:
        self.count -= 1
        return self.count > 0

if __name__ == "__main__":
    pass