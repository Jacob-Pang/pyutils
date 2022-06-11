import time

class RequestLimitBlock:
    def __init__(self, request_limits: list, sorted_limits: bool = False):
        if not sorted_limits:
            request_limits = sorted(request_limits, reverse=True)

        self.request_limit, self.duration = request_limits.pop(0)
        self.child_block = RequestLimitBlock(request_limits,
                sorted_limits=True) if request_limits else None

        self.request_counter = 0
        self.completed_requests = [] # (completed time, no. of requests)

    def get_request_count(self) -> int:
        if not self.child_block:
            return self.request_counter

        return self.request_counter + self.child_block.get_request_count()

    def push_task(self, request_count: int, completed_time: int) -> None:
        self.request_counter += request_count
        self.completed_requests.append((completed_time, request_count))

    def update_block(self, parent_block = None) -> None:
        if self.child_block: # Update child blocks first
            self.child_block.update_block(self)

        while self.completed_requests and self.completed_requests[0][0] \
            < time.time() - self.duration:
            completed_time, request_count = self.completed_requests.pop(0)
            self.request_counter -= request_count

            if parent_block: # Push task to parent
                parent_block.push_task(request_count, completed_time)

    def schedule_requests(self, request_count: int, running_request_count: int) -> int:
        if running_request_count >= self.request_limit:
            return None # No current next available time

        working_request_limit = self.request_limit - running_request_count

        scheduled_time = time.time() if (self.get_request_count() + request_count) \
                < working_request_limit else self.completed_requests[(request_count -
                working_request_limit + self.get_request_count())][0] + self.duration

        if not self.child_block:
            return scheduled_time

        child_scheduled_time = self.child_block.schedule_requests(request_count,
                running_request_count)

        if not child_scheduled_time:
            # No current next available time returned from one point of the chain.
            return None
        
        return max(child_scheduled_time, scheduled_time)
    
    def record_requests(self, request_count: int):
        if self.child_block:
            return self.child_block.record_requests(request_count)
        
        self.completed_requests.append((time.time(), request_count))
        self.request_counter += request_count

class RequestProvider:
    def __init__(self, request_limits: list = None, default_reschedule: int = 60):
        self.running_tasks = {}
        self.running_request_count = 0
        self.default_reschdule = default_reschedule

        self.request_limit_blocks = RequestLimitBlock(request_limits) \
                if request_limits else None

    def schedule_requests(self, request_count: int = 1) -> int:
        if not self.request_limit_blocks:
            return time.time() # No request limits set

        self.request_limit_blocks.update_block()
        scheduled_time = self.request_limit_blocks.schedule_requests(
                request_count, self.running_request_count)

        return scheduled_time if scheduled_time else time.time() \
                + self.default_reschdule

    def run_requests(self, request_id: str, request_count: int) -> None:
        self.running_tasks[request_id] = request_count
        self.running_request_count += request_count

    def complete_requests(self, request_id: str) -> None:
        if self.request_limit_blocks:
            self.request_limit_blocks.record_requests(
                self.running_tasks.get(request_id))
        
        self.running_request_count -= self.running_tasks.pop(request_id)

if __name__ == "__main__":
    pass
