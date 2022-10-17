#pragma once

#include <map>
#include <string>
#include <vector>
#include "Resource.hpp"

static unsigned int resourceProxyCounter = 0;

class ResourceProxy {
    private:
        std::string ID;
        std::vector<Resource> resources;
    
    public:
        ResourceProxy(std::string ID);
        ResourceProxy();

        std::string getID() const;
        int getTimeToNextUpdate() const;

        void registerResource(Resource& resource); // Irreversible action.
        void update();
        bool hasFreeCapacity(int units, size_t* key);
        void use(size_t key, int units);
        void free(size_t key, int units);
};
