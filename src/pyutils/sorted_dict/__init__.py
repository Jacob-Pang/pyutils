from abc import ABC
from abc import abstractmethod

class SortedDictBase (ABC):
    @staticmethod
    def max_comparator(base: any, other: any):
        # Descending
        return base > other

    @staticmethod
    def min_comparator(base: any, other: any):
        # Ascending
        return base < other

    def __init__(self, _comparator: callable = min_comparator) -> None:
        self._comparator = _comparator
        self._kpos_map = dict() # { k: pos }
        self._kv_container = [] # [(k, v) ...]
    
    def __contains__(self, key: any) -> bool:
        return key in self._kpos_map

    def __bool__(self) -> bool:
        return len(self._kpos_map) > 0

    def __getitem__(self, key: any) -> any:
        pos = self._kpos_map[key]
        return self._kv_container[pos][1]
    
    @abstractmethod
    def __setitem__(self, key: any, value: any) -> None:
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self) -> any:
        raise NotImplementedError()

    def _set_position(self, key: any, value: any, pos: int) -> None:
        self._kpos_map[key] = pos
        self._kv_container[pos] = (key, value)

    def keys(self) -> any:
        return self.__iter__()

    def values(self) -> any:
        for key in self:
            yield self[key]

    def items(self) -> tuple[any, any]:
        for key in self:
            yield (key, self[key])

    @abstractmethod
    def popitem(self) -> tuple[any, any]:
        raise NotImplementedError()

    @abstractmethod
    def pop(self, key: any, default: any = None) -> any:
        raise NotImplementedError()

    @abstractmethod
    def front(self) -> tuple[any, any]:
        raise NotImplementedError()

    @abstractmethod
    def back(self) -> tuple[any, any]:
        raise NotImplementedError()

    def copy(self):
        _sorted_dict = type(self)(self._comparator)
        _sorted_dict._kpos_map = self._kpos_map.copy()
        _sorted_dict._kv_container = self._kv_container.copy()

        return _sorted_dict

class SortedDict (SortedDictBase):
    # self._kv_container maintained in descending order
    def __setitem__(self, key: any, value: any) -> None:
        if key in self:
            pos = self._kpos_map[key]
            self._kv_container[pos] = (key, value)
            pos = self._bubble_down(pos)
            self._bubble_up(pos)
            return

        pos = len(self._kv_container)
        self._kv_container.append((key, value))
        self._kpos_map[key] = pos
        self._bubble_up(pos)
        
    def __iter__(self) -> any:
        pos = len(self._kv_container) - 1
        
        while pos >= 0:
            yield self._kv_container[pos][0]
            pos -= 1

    def _bubble_up(self, pos: int) -> int:
        key, value = self._kv_container[pos]

        while pos > 0:
            _key, _value = self._kv_container[pos - 1]

            if not self._comparator(_value, value):
                break
            
            self._set_position(_key, _value, pos)
            pos -= 1

        self._set_position(key, value, pos)
        return pos
        
    def _bubble_down(self, pos: int) -> int:
        key, value = self._kv_container[pos]
        max_pos = len(self._kv_container) - 1

        while pos < max_pos:
            _key, _value = self._kv_container[pos + 1]

            if not self._comparator(value, _value):
                break
            
            self._set_position(_key, _value, pos)
            pos += 1

        self._set_position(key, value, pos)
        return pos

    def popitem(self) -> tuple[any, any]:
        key, value = self._kv_container.pop()
        self._kpos_map.pop(key)

        return (key, value)

    def pop(self, key: any, default: any = None) -> any:
        if key not in self:
            return default

        pos = self._kpos_map[key]
        _pos = pos

        for key, _ in self._kv_container[pos + 1:]:
            self._kpos_map[key] = _pos
            _pos += 1

        return self._kv_container.pop(pos)[1]

    def front(self) -> tuple[any, any]:
        return self._kv_container[-1]
    
    def back(self) -> tuple[any, any]:
        return self._kv_container[0]

if __name__ == "__main__":
    pass