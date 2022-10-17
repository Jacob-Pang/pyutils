#include "intg_pow.hpp"

unsigned long long unsigned_intg_pow_exp_of_two(const unsigned long long base, const unsigned int n,
    const unsigned long long mod) {
    // Returns [base ^ (2 ^ n)] % mod    
    unsigned long long res = base % mod;

    for (int _n = 0; _n < n; _n ++) {
        res = (res * res) % mod;
    };

    return res;
};

unsigned long long unsigned_intg_pow(const unsigned long long base, const unsigned long long exp,
    const unsigned long long mod) {
    // Returns (base ^ exp) % mod
    unsigned int n = 0, rightmost_bit;
    unsigned long long _exp = exp, res = 1;

    while (_exp > 0) {
        rightmost_bit = __builtin_ctzll(_exp) + 1;
        _exp = _exp >> rightmost_bit;

        n += rightmost_bit;
        res *= unsigned_intg_pow_exp_of_two(base, n - 1, mod);
        res %= mod;
    }

    return res;
};

template <typename T,
typename std::enable_if<std::is_integral<T>::value, bool>::type = true>
T intg_pow(const T base, const T exp, const T mod) {
    // Exp and mod must not be negative.
    if (base == 0) return 0;
    if (mod == 0)  return static_cast<T>(pow(static_cast<double>(base),
            static_cast<double>(exp)));
    if (base > 0) return static_cast<T>(unsigned_intg_pow(
            static_cast<const unsigned long long>(base),
            static_cast<const unsigned long long>(exp),
            static_cast<const unsigned long long>(mod)));

    // Negative base
    T res = intg_pow(-base, exp, mod);
    return (exp % 2 == 0) ? res : (-res % mod);
};
