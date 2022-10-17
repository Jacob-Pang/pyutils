from sorted_dict import SortedDictBase

class PriorityDict (SortedDictBase):   
    def __setitem__(self, key: any, value: any) -> None:
        if not key in self:
            pos = len(self._kv_container)
            self._kpos_map[key] = pos
            self._kv_container.append((key, value))
            return self._sift_down(0, pos)
        
        pos = self._kpos_map[key]
        prev_value = self._kv_container[pos][1]
        self._kv_container[pos] = (key, value) # No change in position

        if self._comparator(prev_value, value):
            self._sift_down(0, pos)
        else:
            self._sift_up(pos)

    def __iter__(self) -> any:
        raise NotImplementedError()

    def _sift_down(self, start_pos: int, pos: int) -> None:
        key, value = self._kv_container[pos]

        while pos > start_pos:
            parent_pos = (pos - 1) >> 1
            parent_key, parent_value = self._kv_container[parent_pos]

            if self._comparator(value, parent_value):
                self._set_position(parent_key, parent_value, pos)
                pos = parent_pos
                continue

            break
        
        self._set_position(key, value, pos)

    def _sift_up(self, pos: int) -> None:
        end_pos = len(self._kv_container)
        start_pos = pos
        key, value = self._kv_container[pos]
        child_pos = 2 * pos + 1

        while child_pos < end_pos:
            right_pos = child_pos + 1

            if right_pos < end_pos and not self._comparator(self._kv_container[child_pos][1],
                    self._kv_container[right_pos][1]):
                child_pos = right_pos
            
            child_key, child_value = self._kv_container[child_pos]
            self._set_position(child_key, child_value, pos)
            pos = child_pos
            child_pos = 2 * pos + 1
        
        self._set_position(key, value, pos)
        self._sift_down(start_pos, pos)

    def peekitem(self) -> tuple[any, any]:
        return self._kv_container[0]

    def popitem(self) -> tuple[any, any]:
        key, value = self._kv_container.pop()

        if self._kv_container:
            next_key, next_value = self._kv_container[0]
            self._set_position(key, value, 0)
            self._sift_up(0)

            self._kpos_map.pop(next_key)
            return (next_key, next_value)
        
        self._kpos_map.pop(key)
        return (key, value)

    def pop(self, key: any, default: any = None) -> any:
        if key not in self:
            return default

        pos = self._kpos_map[key]
        value = self._kv_container[pos][1]
        end_pos = len(self._kv_container) - 1

        if pos == end_pos:
            self._kv_container.pop()
        else:
            end_key, end_value = self._kv_container[end_pos]
            self._kv_container.pop()
            self._set_position(end_key, end_value, pos)
            self._sift_up(pos)

        self._kpos_map.pop(key)
        return value
    
    def front(self) -> tuple[any, any]:
        return self._kv_container[0]

    def back(self) -> tuple[any, any]:
        raise Exception() # Back not supported

if __name__ == "__main__":
    pass
