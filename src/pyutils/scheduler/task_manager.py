import time

from multiprocessing.managers import SyncManager
from pyutils.io import erase_stdout
from pyutils.scheduler.resource import ResourceProxy
from pyutils.scheduler.task import Task
from pyutils.scheduler.task.task_state import DoneState, NewState, TaskState, RunningState, BlockedState, WaitingState
from pyutils.scheduler.worker.worker_state import WorkerState, IdleState

class TaskManager:
    def __init__(self, sync_manager: SyncManager, verbose: bool = True) -> None:
        self.verbose = verbose

        self.resources = sync_manager.dict()     # {resource_key: ResourceProxy}
        self.task_states = sync_manager.dict()   # {task_key: TaskState}
        self.worker_states = sync_manager.dict() # {worker_key: WorkerState}

        self.new_tasks_queue = sync_manager.dict() # {task_key: Task}
        self.blocked_tasks_queue = sync_manager.list() # [Task]
        self.waiting_tasks_queue = sync_manager.list() # [Task]

        self.blocked_resource_usage = sync_manager.dict() # {resource_key: blocked_usage}
        self.semaphore = sync_manager.Semaphore(1)

        self.state = sync_manager.Namespace(
            next_task_key=None,
            state_repr_size=0,
            active_public_tasks=0
        )

    def __remove_task_state(self, task_key: str) -> None:
        self.task_states.pop(task_key)
        
    def __free_tasks(self, freed_resource_keys: set) -> None:
        blocked_tasks = len(self.blocked_tasks_queue)

        for _ in range(blocked_tasks):
            task = self.blocked_tasks_queue.pop(0)
            task_state = self.task_states.get(task.key)
            resource_constraints, resource_units = set(), dict()

            for resource_key in task_state.resource_constraints:
                if resource_key in freed_resource_keys:
                    usage = task.resource_usage.get(resource_key)
                    resource_unit = self.resources.get(resource_key).get_free_unit(usage)

                    if resource_unit:
                        resource_units[resource_key] = resource_unit
                        self.blocked_resource_usage[resource_key] -= usage
                        continue

                resource_constraints.add(resource_key)
            
            if resource_constraints:
                task_state = BlockedState(task.key, resource_constraints, private_mode=task.private_mode)
                self.task_states[task.key] = task_state
                self.blocked_tasks_queue.append(task)
                continue

            for resource_key, usage in task.resource_usage.items():
                resource = self.resources.get(resource_key)

                if not resource_key in resource_units:
                    resource_units[resource_key] = resource.get_free_unit(usage)

                resource.use(usage, task.key, resource_units.get(resource_key))
            
            task_state = WaitingState(task.key, resource_units)
            self.task_states[task.key] = task_state
            self.waiting_tasks_queue.append(task)

    def __run_or_block_task(self, task: Task) -> TaskState:
        resource_constraints, resource_units = set(), dict()

        for resource_key, usage in task.resource_usage.items():
            resource = self.resources.get(resource_key)

            if self.blocked_resource_usage.get(resource_key) and usage > 0:
                resource_constraints.add(resource_key)
                continue

            resource_unit = resource.get_free_unit(usage)

            if not resource_unit:
                resource_constraints.add(resource_key)
            else:
                resource_units[resource_key] = resource_unit

        for resource_key, usage in task.resource_usage.items():
            resource = self.resources.get(resource_key)

            if not resource_constraints:
                resource.use(usage, task.key, resource_units.get(resource_key))
            else: # Enqueue usage
                self.blocked_resource_usage[resource_key] += usage

        return BlockedState(task.key, resource_constraints, private_mode=task.private_mode) \
                if resource_constraints else \
                RunningState(task.key, resource_units, private_mode=task.private_mode)

    def __update_next_task_key(self) -> None:
        self.state.next_task_key = None
        timestamp = None
        
        for task_key in self.new_tasks_queue.keys():
            task_state = self.task_states.get(task_key)

            if self.state.next_task_key is None or task_state.timestamp < timestamp:
                self.state.next_task_key = task_key
                timestamp = task_state.timestamp

    def __str__(self) -> str:
        return f"TIME : {time.time()}\n" + \
                "-------------------------------------------------------------------------------------------------------------------\n" + \
                "           ResourceID             UnitID                 Usage / Capacity\n" + \
                "\n".join([str(resource) for resource in self.resources.values()]) + "\n\n" + \
                "           WorkerID                                      State       TaskID                      Duration\n" + \
                "\n".join([str(worker_state) for worker_state in self.worker_states.values()]) + "\n\n" + \
                "           TaskID                                        State       Timestamp\n" + \
                "\n".join([
                    str(task_state) for task_state in self.task_states.values()
                    if not task_state.private_mode
                ]) + "\n"

    def __update_task_manager_state(self) -> None:
        if not self.verbose: return
        erase_stdout(self.state.state_repr_size)

        state_repr = str(self)
        self.state.state_repr_size = state_repr.count('\n') + 1
        print(state_repr)

    def register_resource(self, resource: ResourceProxy) -> None:
        self.resources[resource.key] = resource
        self.blocked_resource_usage[resource.key] = 0

    def register_task(self, task: Task, timestamp: float = None) -> None:
        self.task_states[task.key] = NewState(task.key, timestamp, task.private_mode)
        self.new_tasks_queue[task.key] = task

        self.state.next_task_key = None
        
        if not task.private_mode:
            self.state.active_public_tasks += 1

    def register_worker(self, worker_key: str) -> None:
        self.worker_states[worker_key] = IdleState(worker_key)

    def update_worker_state(self, worker_state: WorkerState) -> None:
        self.worker_states[worker_state.key] = worker_state
        self.__update_task_manager_state()

    def dispatch_task(self) -> Task:
        if self.waiting_tasks_queue:
            task = self.waiting_tasks_queue.pop(0)

            task_state = self.task_states.get(task.key)
            self.task_states[task.key] = RunningState(task.key, task_state.resource_units,
                    private_mode=task.private_mode)

            self.__update_task_manager_state()
            return task

        task, state_change = None, False

        while self.new_tasks_queue:
            if self.state.next_task_key is None:
                self.__update_next_task_key()

            task_key = self.state.next_task_key

            if self.task_states.get(task_key).timestamp > time.time():
                break

            state_change = True
            task = self.new_tasks_queue.pop(task_key)
            self.state.next_task_key = None

            task_state = self.__run_or_block_task(task)
            self.task_states[task_key] = task_state

            if isinstance(task_state, RunningState):
                break

            self.blocked_tasks_queue.append(task)
            task = None

        if state_change:
            self.__update_task_manager_state()

        return task

    def post_update(self, task: Task, task_state: TaskState) -> None:
        resource_units = self.task_states.get(task.key).resource_units
        self.task_states[task.key] = task_state
        update_tasks = dict()

        if isinstance(task_state, NewState): # Requeue
            self.new_tasks_queue[task.key] = task
        else:
            if not task.private_mode:
                self.state.active_public_tasks -= 1

            if isinstance(task_state, DoneState) and task_state.remove_state:
                self.__remove_task_state(task.key)

        if not resource_units:
            return

        for resource_key, resource_unit in resource_units.items():
            resource = self.resources.get(resource_key)
            usage = task.resource_usage.get(resource_key)
            resource.free(usage, task.key, resource_unit, update_tasks)

        for update_task, timestamp in update_tasks.items():
            self.register_task(update_task, timestamp)

        self.__free_tasks(resource_units.keys())
        self.__update_task_manager_state()

    def active_tasks(self, public_only: bool = True) -> int:
        if public_only:
            return self.state.active_public_tasks
        
        return len(self.new_tasks_queue) + len(self.blocked_tasks_queue) + \
                len(self.waiting_tasks_queue)

if __name__ == "__main__":
    pass