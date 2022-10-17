#include "Task.hpp"

Task::Task() {};

Task::Task(TargetFunction& target, const std::string& ID, time_t startTime,
        const std::unordered_map<std::string, unsigned int> resourceUsage) {

    this->target = target;
    this->ID = (ID != "") ? ID: std::to_string(taskCounter);
    this->startTime = (startTime > time(0)) ? startTime : time(0);
    this->resourceUsage = resourceUsage;

    taskCounter ++;
};

void Task::operator()(ResourceManager& resourceManager) const {
    this->target(); // Executes target
    resourceManager.recordFreedResources(this->resourceUsage);
};

std::string Task::getID() const {
    return this->ID;
};
