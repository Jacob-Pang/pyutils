# distutils: language = c++

np.import_array()

cdef np.ndarray _ndarray_as_numpy(void* _array, np.npy_intp * shape, int dtype_num):
    return np.PyArray_SimpleNewFromData(1, shape, dtype_num, _array)

cdef np.ndarray _array_as_numpy(T _array, int size, int dtype_num):
    cdef np.npy_intp shape[1]
    shape[0] = size

    return _ndarray_as_numpy(_array, shape, dtype_num)

cdef np.ndarray array_as_npy_int(void* _array, int size):
    return _array_as_numpy(_array, size, np.NPY_INT)

cdef np.ndarray array_as_npy_float(void* _array, int size):
    return _array_as_numpy(_array, size, np.NPY_FLOAT)

cdef np.ndarray array_as_npy_double(void* _array, int size):
    return _array_as_numpy(_array, size, np.NPY_DOUBLE)
