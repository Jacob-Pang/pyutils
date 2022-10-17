#pragma once

#include <string>
#include <ctime>
#include <unordered_map>
#include "TargetFunction.hpp"
#include "resource/ResourceManager.hpp"

static unsigned int taskCounter = 0;

class Task {
    private:
        TargetFunction target;
        std::string ID;
        time_t startTime;
        std::unordered_map<std::string, unsigned int> resourceUsage;
    
    public:
        Task();
        Task(TargetFunction& target, const std::string& ID = "", time_t startTime = 0,
                const std::unordered_map<std::string, unsigned int> resourceUsage =
                std::unordered_map<std::string, unsigned int>());
        
        void operator()(ResourceManager& resourceManager) const;
        std::string getID() const;
};
