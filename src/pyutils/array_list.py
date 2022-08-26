import numpy as np

class FixedArrayList:
    def __init__(self, shape: int, init_array_values: np.ndarray = None, dtype: any = None) -> None:
        self.shape = shape
        self.head = 0
        self.size = 0

        self.data = np.empty(
            shape=shape, dtype=(dtype if init_array_values is None else init_array_values.dtype)
        )

        if not init_array_values is None:
            self.data[:init_array_values.shape[0]] = init_array_values
            self.size = init_array_values.shape[0]

    def __get_tail_index(self) -> int:
        return (self.head + self.size) % self.shape[0]

    def push_back(self, value: any) -> None:
        tail = self.__get_tail_index()
        self.data[tail] = value

        if tail == self.head and self.size:
            self.head = (self.head + 1) % self.shape[0]

        self.size = min(self.size + 1, self.shape[0])

    def push_front(self, value: any) -> None:
        self.head = (self.head if self.head else self.shape[0]) - 1
        self.data[self.head] = value

        self.size = min(self.size + 1, self.shape[0])

    def pop_back(self) -> any:
        tail = self.__get_tail_index()
        self.size -= 1

        return self.data[tail]

    def pop_front(self) -> any:
        head = self.head
        self.size -= 1
        self.head = (self.head + 1) % self.shape[0]

        return self.data[head]

    def get(self, index: int) -> any:
        return self.data[(index + self.head) % self.shape[0]]

    def set(self, index: int, value: any) -> None:
        self.data[(index + self.head) % self.shape[0]] = value

    def get_array(self) -> np.ndarray:
        tail = self.__get_tail_index()

        if not self.size:
            return np.array([])

        if tail > self.head:
            return self.data[self.head:tail]
        
        return np.concatenate([self.data[self.head:], self.data[:tail]], axis=0)

    def __str__(self) -> str:
        return self.get_array().__str__()

if __name__ == "__main__":
    pass