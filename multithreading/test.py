from PyTask import test
from _PyTask import PyTask

def f(x):
    return x

pytask = PyTask(f, x=1)
test(pytask)

print(pytask.get_future().get())
