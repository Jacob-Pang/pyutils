# distutils: language = c++

import numpy as np
cimport numpy as np

cdef np.ndarray _ndarray_as_numpy(void* _array, np.npy_intp * shape, int dtype_num)
cdef np.ndarray _array_as_numpy(void* _array, int size, int dtype_num)

cdef np.ndarray array_as_npy_int(void* _array, int size)
cdef np.ndarray array_as_npy_float(void* _array, int size)
cdef np.ndarray array_as_npy_double(void* _array, int size)
