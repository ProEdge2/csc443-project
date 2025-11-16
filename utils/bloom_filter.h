#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <vector>
#include <string>
#include <cmath>
#include <functional>

template <typename K>
class BloomFilter {
public:
    BloomFilter(size_t expected_elements, double false_positive_rate) {
        // optimal number of bits (m)
        // m = -(n * ln(p)) / (ln(2)^2)
        num_bits = -static_cast<size_t>((expected_elements * std::log(false_positive_rate)) / (std::log(2) * std::log(2)));
        // Ensure at least one bit
        if (num_bits == 0) {
            num_bits = 1;
        }

        // optimal number of hash functions (k)
        // k = (m/n) * ln(2)
        num_hash_functions = static_cast<size_t>((static_cast<double>(num_bits) / expected_elements) * std::log(2));
        // Ensure at least one hash function
        if (num_hash_functions == 0) {
            num_hash_functions = 1;
        }
        bit_array.resize(num_bits, false);
    }

    // Constructor
    BloomFilter(size_t bits, size_t hash_funcs, const std::vector<char>& data)
        : num_bits(bits), num_hash_functions(hash_funcs) {
        bit_array.resize(num_bits);
        // reverse serialization done during write
        // shift bytes and fill corresponding positions
        for (size_t i = 0; i < num_bits; ++i) {
            if ((data[i / 8] >> (i % 8)) & 1) {
                bit_array[i] = true;
            } else {
                bit_array[i] = false;
            }
        }
    }

    void add(const K& key) {
        if (num_bits == 0) return;
        size_t h1 = std::hash<K>{}(key);
        // create second hash from first hash by xor with golden ratio constant (commonly used to reduce clustering and make independent hash)
        size_t h2 = std::hash<size_t>{}(h1 ^ 0x9e3779b97f4a7c15ULL);

        // compute num hash functions number of hashes and update the corresponding bits to true
        for (size_t i = 0; i < num_hash_functions; ++i) {
            size_t index = (h1 + i * h2) % num_bits;
            bit_array[index] = true;
        }
    }

    bool contains(const K& key) const {
        if (num_bits == 0) {
            return false;
        }
        size_t h1 = std::hash<K>{}(key);
        size_t h2 = std::hash<size_t>{}(h1 ^ 0x9e3779b97f4a7c15ULL);

        for (size_t i = 0; i < num_hash_functions; ++i) {
            size_t index = (h1 + i * h2) % num_bits;
            // If false, return right away
            if (!bit_array[index]) {
                return false;
            }
        }
        return true;
    }

public:
    std::vector<bool> bit_array;
    size_t num_bits;
    size_t num_hash_functions;
};

#endif
