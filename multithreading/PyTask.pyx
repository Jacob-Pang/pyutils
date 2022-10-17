# distutils: language = c++

cdef void exec_pytask(void* _pytask):
    (<object>_pytask)()

cdef void testy(void * f):

    cdef Dummy d = Dummy()
    d.execute(Task(TargetFunction(exec_pytask, f)))

def test(f):
    print("start")
    testy(<void *> f)
    # Task()()