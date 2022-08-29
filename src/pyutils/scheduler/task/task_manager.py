import heapq
import time

from multiprocessing.managers import SyncManager, ListProxy

from pyutils.io import erase_stdout
from pyutils.wrapper import WrapperMetaclass
from pyutils.scheduler.resource import Resource
from pyutils.scheduler.task import Task
from pyutils.scheduler.state.task_state import NewState, WaitingState, RunningState, DoneState, BlockedState
from pyutils.scheduler.state.worker_state import DeadState, WorkerState, IdleState

class WrappedListProxy (list, metaclass=WrapperMetaclass, wrapped_class=ListProxy):
    def __init__(self, sync_manager: SyncManager) -> None:
        self.list_proxy = sync_manager.list()

    @property
    def wrapped_object(self) -> ListProxy:
        return self.list_proxy

class TaskManager:
    def __init__(self, sync_manager: SyncManager, verbose: bool = True) -> None:
        self.verbose = verbose
        self.resources = sync_manager.dict() # {resource_key: Resource}
        self.task_states = sync_manager.dict() # {task_key: TaskState}
        self.worker_states = sync_manager.dict() # {worker_key: WorkerState}
        self.new_tasks = WrappedListProxy(sync_manager) # [(timestamp, Task)]
        self.blocked_tasks = sync_manager.list()
        self.waiting_tasks = sync_manager.list()
        self.blocked_resource_usage = sync_manager.dict() # {resource_key: blocked_usage}
        self.semaphore = sync_manager.Semaphore(1)
        self.manager_state = sync_manager.Namespace(
            description="",
            state_repr_size=0,
            public_pending_tasks=0,
            public_active_tasks=0,
            active_workers=0
        )

    @property
    def description(self) -> str:
        return self.manager_state.description

    @description.setter
    def description(self, _description: str) -> None:
        self.manager_state.description = _description

    @property
    def public_pending_tasks(self) -> int:
        return self.manager_state.public_pending_tasks

    @property
    def public_active_tasks(self) -> int:
        return self.manager_state.public_active_tasks

    @property
    def active_workers(self) -> int:
        return self.manager_state.active_workers

    # Registration methods
    def register_resource(self, sync_manager: SyncManager, resource: Resource) -> None:
        self.resources[resource.key] = resource.as_shared_proxy(sync_manager)
        self.blocked_resource_usage[resource.key] = 0

    def register_task(self, task: Task, timestamp: float = None) -> None:
        task_state = task.create_task_state(NewState, timestamp)

        self.task_states[task.key] = task_state
        print(self.new_tasks)
        heapq.heappush(self.new_tasks, (task_state.timestamp, task))
        print(self.new_tasks)
        
        if not task.private:
            self.manager_state.public_pending_tasks += 1
            self.manager_state.public_active_tasks += 1

    def register_worker(self, worker_key: str) -> None:
        self.worker_states[worker_key] = IdleState(worker_key)
        self.manager_state.active_workers += 1

    # Execution methods
    def process_new_tasks(self) -> None:
        def process_new_task(task: Task) -> None:
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

            if resource_constraints: # Push to blocked_tasks
                for resource_key, usage in task.resource_usage.items():
                    self.blocked_resource_usage[resource_key] += usage

                self.task_states[task.key] = task.create_task_state(BlockedState, resource_constraints=resource_constraints)
                return self.blocked_tasks.append(task)
            
            # Push to waiting_tasks
            for resource_key, usage in task.resource_usage.items():
                resource = self.resources.get(resource_key)
                resource.use(usage, task.key, resource_units.get(resource_key))
            
            self.task_states[task.key] = task.create_task_state(WaitingState, resource_units=resource_units)
            self.waiting_tasks.append(task)
        
        change_in_state = False

        while len(self.new_tasks):
            timestamp, task = self.new_tasks[0]

            if timestamp > time.time():
                break # Processing timestamp constraint

            heapq.heappop(self.new_tasks)
            process_new_task(task)
            change_in_state = True

        if change_in_state:
            self.log_task_manager_state()

    def free_blocked_tasks(self, freed_resource_keys: set) -> None:
        blocked_tasks_count = len(self.blocked_tasks)

        for _ in range(blocked_tasks_count):
            task = self.blocked_tasks.pop(0)
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
                task_state = task.create_task_state(BlockedState, resource_constraints=resource_constraints)
                self.task_states[task.key] = task_state
                self.blocked_tasks.append(task)
                continue

            for resource_key, usage in task.resource_usage.items():
                resource = self.resources.get(resource_key)

                if not resource_key in resource_units:
                    resource_units[resource_key] = resource.get_free_unit(usage)

                resource.use(usage, task.key, resource_units.get(resource_key))
            
            task_state = task.create_task_state(WaitingState, resource_units=resource_units)
            self.task_states[task.key] = task_state
            self.waiting_tasks.append(task)

    def __str__(self) -> str:
        return  f"{self.description}\n" + \
                f"Timestamp   : {int(time.time())}\n" + \
                f"ActiveTasks : {self.public_active_tasks}\n" + \
                "-------------------------------------------------------------------------------------------------------------------\n" + \
                "           ResourceID             UnitID                 Usage / Capacity\n" + \
                "\n".join([str(resource) for resource in self.resources.values()]) + "\n\n" + \
                "           WorkerID                                      State       TaskID                      Duration\n" + \
                "\n".join([str(worker_state) for worker_state in self.worker_states.values()]) + "\n\n" + \
                "           TaskID                                        State       Timestamp         Run\n" + \
                "\n".join([
                    str(task_state) for task_state in self.task_states.values()
                    if task_state.visible
                ]) + "\n"

    def log_task_manager_state(self) -> None:
        if not self.verbose: return
        erase_stdout(self.manager_state.state_repr_size)

        state_repr = str(self)
        self.manager_state.state_repr_size = state_repr.count('\n') + 1
        print(state_repr)

    def get_waiting_tasks_count(self) -> int:
        return len(self.waiting_tasks)

    # Update methods
    def update_worker_state(self, worker_state: WorkerState) -> None:
        self.worker_states[worker_state.key] = worker_state

        if isinstance(worker_state, DeadState):
            self.manager_state.active_workers -= 1

            if worker_state.remove_worker_state:
                self.worker_states.pop(worker_state.key)

        self.log_task_manager_state()

    def dispatch_task(self) -> Task:
        self.process_new_tasks()

        if not len(self.waiting_tasks):
            return None

        task = self.waiting_tasks.pop(0)

        if not task.private: # Update transition from waiting to running.
            self.manager_state.public_pending_tasks -= 1

        task_state = self.task_states.get(task.key)
        self.task_states[task.key] = task.create_task_state(RunningState, resource_units=task_state.resource_units)    
        self.log_task_manager_state()

        return task

    def post_update(self, task: Task, task_state: DoneState) -> None:
        resource_units = self.task_states.get(task.key).resource_units
        self.task_states[task.key] = task_state

        if not task.private_mode:
            self.manager_state.public_active_tasks -= 1

        if task_state.remove_task_state:
            self.task_states.pop(task.key)

        for _task, timestamp in task_state.tasks_to_register.items():
            self.register_task(_task, timestamp)

        # Update resources and generate resource-update tasks
        if not resource_units: return
        update_tasks = dict()

        for resource_key, resource_unit in resource_units.items():
            resource = self.resources.get(resource_key)
            usage = task.resource_usage.get(resource_key)
            resource.free(usage, task.key, resource_unit, update_tasks)

        for _task, timestamp in update_tasks.items():
            self.register_task(_task, timestamp)

        self.free_blocked_tasks(resource_units.keys())
        self.log_task_manager_state()

if __name__ == "__main__":
    pass