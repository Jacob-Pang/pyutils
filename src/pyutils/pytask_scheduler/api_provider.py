import time

# TODO: Generalize limits

class APIProvider:
    def init(self, api_provider_id: str, request_limit_per_min: int = None,
        request_limit_per_hour: int = None):

        self.api_provider_id = api_provider_id
        self.request_limit_per_min = request_limit_per_min
        self.request_limit_per_hour = request_limit_per_hour

        # Request tracking queues
        self.min_request_queue = []
        self.hour_request_queue = []

    def __hash__(self) -> int:
        return self.api_provider_id.__hash__()

    def log_api_request(self) -> None:
        self.min_request_queue.append(time.time())

    def update_queues(self) -> None:
        while self.min_request_queue and self.min_request_queue[0] < time.time() - 60:
            self.hour_request_queue.append(self.min_request_queue.pop(0))

        while self.hour_request_queue and self.hour_request_queue[0] < time.time() - 3600:
            self.hour_request_queue.pop(0)

    def schedule_request_time(self) -> int:
        self.update_queues() # Lazy updating upon request only.

        # Based on minute request limits
        min_schedule_time = self.min_request_queue[0] + 60 if self.request_limit_per_min and \
                len(self.min_request_queue) >= self.request_limit_per_min else time.time()

        hour_schedule_time = self.hour_request_queue[0] + 3600 if self.request_limit_per_hour and \
                (len(self.min_request_queue) + len(self.hour_request_queue)) >= \
                self.request_limit_per_hour else time.time()

        return max(min_schedule_time, hour_schedule_time)

if __name__ == "main":
    pass
