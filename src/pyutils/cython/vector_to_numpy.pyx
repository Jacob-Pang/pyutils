# distutils: language = c++

import numpy as np
cimport numpy as np
from libcpp.vector cimport vector

np.import_array()

ctypedef fused T:
    int
    float
    double

cdef np.ndarray ndvector_as_numpy(vector[T] _vector, np.npy_intp * shape):
    cdef void* _array = &_vector[0]
    cdef int dtype_num

    if T is int:
        dtype_num = np.NPY_INT
    elif T is float:
        dtype_num = np.NPY_FLOAT
    elif T is double:
        dtype_num = np.NPY_DOUBLE

    return np.PyArray_SimpleNewFromData(1, shape, dtype_num, _array)

cdef np.ndarray vector_as_numpy(vector[T] _vector):
    cdef np.npy_intp shape[1]
    shape[0] = _vector.size()

    return ndvector_as_numpy(_vector, shape)
