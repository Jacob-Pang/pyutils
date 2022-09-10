# distutils: language = c++

import numpy as np
cimport numpy as np

from libcpp.vector cimport vector

ctypedef fused T:
    int
    float
    double

cdef np.ndarray ndvector_as_numpy(vector[T] _vector, np.npy_intp * shape)
cdef np.ndarray vector_as_numpy(vector[T] _vector)
