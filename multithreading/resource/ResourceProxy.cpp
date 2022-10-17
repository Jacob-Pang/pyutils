#include "ResourceProxy.hpp"

ResourceProxy::ResourceProxy(std::string ID) {
    this->ID = ID;
    resourceProxyCounter ++;
};

ResourceProxy::ResourceProxy() :
        ResourceProxy::ResourceProxy(std::to_string(resourceProxyCounter)) {
};

std::string ResourceProxy::getID() const {
    return this->ID;
};

int ResourceProxy::getTimeToNextUpdate() const {
    int minTime = 300; // default update frequency

    for (const Resource& resource: this->resources) {
        int time = resource.getTimeToNextUpdate();

        if (time < minTime)
            minTime = time;
    }

    return minTime;
};

void ResourceProxy::registerResource(Resource& resource) {
    this->resources.push_back(resource);
};

void ResourceProxy::update() {
    for (Resource& resource: this->resources) {
        resource.update();
    }
};

bool ResourceProxy::hasFreeCapacity(int units, size_t* key) {
    // update called implicitly during Resource.hasFreeCapacity
    for (size_t _key = 0, end = this->resources.size(); _key < end; _key ++) {
        if (this->resources.at(_key).hasFreeCapacity(units)) {
            *key = _key;
            return true;
        }
    }

    return false;
};
      
void ResourceProxy::use(size_t key, int units) {
    this->resources[key].use(units);
};

void ResourceProxy::free(size_t key, int units) {
    this->resources[key].free(units);
};
