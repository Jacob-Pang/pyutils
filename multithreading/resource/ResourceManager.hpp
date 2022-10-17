#pragma once

#include <deque>
#include <semaphore>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include "ResourceProxy.hpp"

class ResourceManager {
    private:
        // Accessed by TaskManager only
        std::unordered_map<std::string, ResourceProxy> resources;
        std::unordered_map<std::string, unsigned int> reservedUsage;

        // Accessed by Task only
        // Buffer to record (un-updated) freed resources
        std::binary_semaphore freeUsageSem = std::binary_semaphore(0);
        std::unordered_map<std::pair<std::string, size_t>, unsigned int> freedUsage;

    public:
        ResourceManager();

        int getTimeToNextUpdate() const;
        
        // Mutators
        void registerResource(ResourceProxy& resource);
        void registerResource(const std::string& ID, Resource& resource);

        bool useOrReserve(const std::unordered_map<std::string, unsigned int>& usage,
                std::unordered_map<std::string, size_t>& assignedKeys,
                std::unordered_set<std::string>& constraints);

        bool useReservedOrUpdate(const std::unordered_map<std::string, unsigned int>& usage,
                std::unordered_map<std::string, size_t>& assignedKeys,
                std::unordered_set<std::string>& constraints);

        void recordFreedResources(const std::unordered_map<std::string, unsigned int>& usage,
                const std::unordered_map<std::string, size_t>& assignedKeys);
        
        void update(std::unordered_set<std::string>& updatedResources);
};
