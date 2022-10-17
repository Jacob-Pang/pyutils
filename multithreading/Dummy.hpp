#include "Task.hpp"

class Dummy {
public:
    Dummy() {};

    void execute(Task& task) {
        task();
    };
};
