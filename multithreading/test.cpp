#include "Task.hpp"
#include "Task.cpp"

void test() {
    printf("???\n");
};

int main() {
    TargetFunction f = TargetFunction(test);
    f();
    Task t = Task(f);
    t();

    return 0;
};