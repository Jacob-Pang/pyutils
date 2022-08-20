import random
import time

from collections.abc import Iterable

class RequestProviderGate:
    def __init__(self, usage_limits: Iterable, gate_id: str = None, gate_keys: any = None) -> None:
        """
        Parameters:
            usage_limits (Iterable): Collection of (usage_capacity, usage_window).
        """
        if not gate_id: gate_id = f"gate_{int(time.time())}_{int(random.random() * 1e5)}"

        self.usage_limits = usage_limits
        self.gate_id = gate_id
        self.gate_keys = gate_keys

        self.requests_in_progress = dict() # request_id: requests
        self.requests_history = [] # (timestamp, requests)

    def get_requests_in_progress_count(self) -> int:
        return sum(self.requests_in_progress.values())

    def purge_requests_history(self) -> None:
        max_window = max([usage_window for _, usage_window in self.usage_limits])
        boundary = time.time() - max_window

        while self.requests_history:
            timestamp, _ = self.requests_history[0]
            if timestamp >= boundary: return
            self.requests_history.pop(0)

    def get_spare_usage_capacity(self) -> int:
        # Returns the number of requests that can be processed.
        self.purge_requests_history()
        requests_in_progress = self.get_requests_in_progress_count()
        current_timestamp = time.time()

        def _get_spare_usage_capacity(usage_capacity: int, usage_window: int) -> int:
            boundary = current_timestamp - usage_window
            used_capacity = requests_in_progress

            for timestamp, requests in reversed(self.requests_history):
                if timestamp < boundary: break
                used_capacity += requests
            
            return max(usage_capacity - used_capacity, 0)

        return min([
            _get_spare_usage_capacity(usage_capacity, usage_window)
            for usage_capacity, usage_window in self.usage_limits
        ])

    def get_next_available_timestamp(self, requests: int, default_timestamp: int = time.time() + 60) -> int:
        # Returns the timestamp where the number of requests can be accomodated
        # and <default_timestamp> if no such timestamp is available.
        self.purge_requests_history()
        requests_in_progress = self.get_requests_in_progress_count()
        current_timestamp = time.time()

        def _get_next_available_timestamp(requests: int, usage_capacity: int, usage_window: int) -> int:
            boundary = current_timestamp - usage_window
            spare_capacity = usage_capacity - requests_in_progress
            if requests > spare_capacity: return default_timestamp

            for timestamp, requests_ in self.requests_history:
                if timestamp < boundary or not spare_capacity: break
                spare_capacity = max(spare_capacity - requests_, 0)
            
            if spare_capacity: return current_timestamp

            for timestamp, requests_ in self.requests_history:
                requests = max(requests - requests_, 0)

                if not requests:
                    return timestamp + usage_window
        
        return max([
            _get_next_available_timestamp(requests, usage_capacity, usage_window)
            for usage_capacity, usage_window in self.usage_limits
        ])

    def process_requests(self, requests_id: str, requests: int) -> None:
        spare_usage_capacity = self.get_spare_usage_capacity()
        assert spare_usage_capacity >= requests, f"""
        RequestProviderGate: attempted to process requests {requests} without
                sufficient usage capacity: {spare_usage_capacity}.
        """
        self.requests_in_progress[requests_id] = requests

    def complete_requests(self, requests_id: int, update_history: bool = False) -> None:
        requests = self.requests_in_progress.pop(requests_id)

        if update_history:
            self.requests_history.append((time.time(), requests))

    def __hash__(self) -> int:
        return self.gate_id.__hash__()
    
class RequestProvider:
    def __init__(self, usage_limits: Iterable) -> None:
        """
        Parameters:
            usage_limits (Iterable): Collection of (usage_capacity, usage_window).
        """
        self.usage_limits = usage_limits
        self.gates = set()
    
    def create_gate(self, gate_id: str = None, gate_keys: any = None) -> None:
        self.gates.add(RequestProviderGate(self.usage_limits, gate_id, gate_keys))

    def get_next_available_timestamp(self, requests: int, default_timestamp: int = time.time() + 60) -> tuple:
        min_gate, min_timestamp = None, None

        for gate in self.gates:
            timestamp = gate.get_next_available_timestamp(requests, default_timestamp)

            if not min_gate or timestamp < min_timestamp:
                min_gate, min_timestamp = gate, timestamp
        
        return min_gate, min_timestamp

if __name__ == "__main__":
    pass
