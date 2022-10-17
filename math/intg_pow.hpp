#include <type_traits>
#include <math.h>

template <typename T,
typename std::enable_if<std::is_integral<T>::value, bool>::type = true>
T intg_pow(const T base, const T exp, const T mod);
