# distutils: language = c++

import numpy as np
cimport numpy as np

ctypedef fused T:
    int
    float
    double

cdef np.ndarray ndarray_as_numpy(T* _array, np.npy_intp * shape)
cdef np.ndarray array_as_numpy(T* _array, int size)
