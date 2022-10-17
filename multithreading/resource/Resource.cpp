#include "Resource.hpp"

Resource::Resource() {};

Resource::Resource(const unsigned int capacity) {
    this->capacity = capacity;
};

int Resource::getTimeToNextUpdate() const {
    return 300; // default update frequency
};

void Resource::update() {
    // No update procedure set.
};

bool Resource::hasFreeCapacity(int units) {
    Resource::update();
    return this->capacity >= this->usage + units;
};

void Resource::use(int units) {
    this->usage += units;
};

void Resource::free(int units) {
    this->usage -= units;
};
