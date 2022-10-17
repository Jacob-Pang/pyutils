# distutils: sources = Task.cpp

from libcpp.string cimport string

cdef extern from "TargetFunction.hpp":
    cdef cppclass TargetFunction:
        TargetFunction(void (*function)(void*), void*) except +

cdef extern from "Task.cpp":
    pass

cdef extern from "Task.hpp":
    cdef cppclass Task:
        TargetFunction target
        string ID
        long long startTime

        Task() except +
        Task(TargetFunction&, const string&, long long) except +
        Task(TargetFunction&) except +

        void operator()() const

cdef extern from "Dummy.hpp":
    cdef cppclass Dummy:
        Dummy() except +

        void execute(Task& task)
