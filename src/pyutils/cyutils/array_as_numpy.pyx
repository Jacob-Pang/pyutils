# distutils: language = c++

np.import_array()

cdef np.ndarray ndarray_as_numpy(T _array, np.npy_intp * shape):
    cdef int dtype_num

    if T is int *:
        dtype_num = np.NPY_INT
    elif T is float *:
        dtype_num = np.NPY_FLOAT
    elif T is double *:
        dtype_num = np.NPY_DOUBLE

    return np.PyArray_SimpleNewFromData(1, shape, dtype_num, _array)

cdef np.ndarray array_as_numpy(T _array, int size):
    cdef np.npy_intp shape[1]
    shape[0] = size

    return ndarray_as_numpy(_array, shape)
