#include <queue>
#include <map>

#include "Task.hpp"

class TaskManager {
    private:
        std::priority_queue<Task> taskQueue;
        std::unordered_map<std::string, Task> blockedTasks;

        // Maps taskID to { resourceID: constraint }
        std::unordered_map<std::string, std::unordered_map<std::string, unsigned int>> resourceConstraints;

    public:
        TaskManager();

        void submit(Task& task);
};
