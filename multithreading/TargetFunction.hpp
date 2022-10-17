#pragma once

#include <functional>

class TargetFunction {
    // Wrapper class for generic function signatures
    private:
        std::function<void()> _function;

    public:
        TargetFunction() {};

        template<typename Function, typename ... ArgTypes>
        TargetFunction(Function function, ArgTypes ... args) :
                _function([=]{ function(args ...); }) {};

        void operator()() const {
            this->_function();
        };
};
