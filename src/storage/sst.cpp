#ifndef SST_CPP
#define SST_CPP

#include "sst.h"
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <sys/stat.h>

template<typename K, typename V>
SST<K, V>::SST(const std::string& file_path)
    : filename(file_path), entry_count(0) {
}

template<typename K, typename V>
SST<K, V>::~SST() {
}

template<typename K, typename V>
bool SST<K, V>::create_from_memtable(const std::string& file_path,
                                    const std::vector<std::pair<K, V>>& sorted_data) {
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    try {
        // Update SST metadata
        filename = file_path;
        entry_count = sorted_data.size();

        if (entry_count == 0) {
            file.close();
            return true; // Empty SST file created successfully
        }

        // Set min and max keys
        min_key = sorted_data[0].first;
        max_key = sorted_data[entry_count - 1].first;

        // Write data to file in binary format
        // For simplicity, we'll write each key-value pair sequentially
        for (const auto& pair : sorted_data) {
            // Write key
            file.write(reinterpret_cast<const char*>(&pair.first), sizeof(K));
            // Write value
            file.write(reinterpret_cast<const char*>(&pair.second), sizeof(V));
        }

        file.close();
        return true;

    } catch (const std::exception& e) {
        std::cerr << "Error creating SST file: " << e.what() << std::endl;
        file.close();
        return false;
    }
}

template<typename K, typename V>
bool SST<K, V>::get(const K& key, V& value) const {
    if (entry_count == 0 || key < min_key || key > max_key) {
        return false;
    }

    // TODO:
}

template<typename K, typename V>
std::vector<std::pair<K, V>> SST<K, V>::scan(const K& start_key, const K& end_key) const {
    std::vector<std::pair<K, V>> results;

    if (entry_count == 0 || start_key > max_key || end_key < min_key) {
        return results;
    }

    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        return results;
    }

    try {
        // Find the starting position using binary search
        size_t start_pos = binary_search_start_position(file, start_key);

        // Read from start position until we find a key > end_key
        file.seekg(start_pos * (sizeof(K) + sizeof(V)));

        for (size_t i = start_pos; i < entry_count; i++) {
            K key;
            V value;

            file.read(reinterpret_cast<char*>(&key), sizeof(K));
            file.read(reinterpret_cast<char*>(&value), sizeof(V));

            if (key > end_key) {
                break; // We've gone past the end of our range
            }

            if (key >= start_key) {
                results.push_back(std::make_pair(key, value));
            }
        }

        file.close();
    } catch (const std::exception& e) {
        std::cerr << "Error scanning SST file: " << e.what() << std::endl;
        file.close();
    }

    return results;
}

template<typename K, typename V>
bool SST<K, V>::binary_search_file(const K& target_key, V& value) const {
    // TODO: ...
    return false;
}

template<typename K, typename V>
size_t SST<K, V>::binary_search_start_position(std::ifstream& file, const K& start_key) const {
    size_t left = 0;
    size_t right = entry_count;
    size_t result = entry_count; // Default to end if not found

    while (left < right) {
        size_t mid = left + (right - left) / 2;

        // Read key at position mid
        file.seekg(mid * (sizeof(K) + sizeof(V)));
        K key;
        file.read(reinterpret_cast<char*>(&key), sizeof(K));

        if (key >= start_key) {
            result = mid;
            right = mid;
        } else {
            left = mid + 1;
        }
    }

    return result;
}

template<typename K, typename V>
bool SST<K, V>::load_existing_sst(const std::string& file_path,
                                 std::unique_ptr<SST<K, V>>& sst_ptr) {
    // TODO: ...
    return false;
}

template<typename K, typename V>
const std::string& SST<K, V>::get_filename() const {
    return filename;
}

template<typename K, typename V>
size_t SST<K, V>::get_entry_count() const {
    return entry_count;
}

template<typename K, typename V>
const K& SST<K, V>::get_min_key() const {
    return min_key;
}

template<typename K, typename V>
const K& SST<K, V>::get_max_key() const {
    return max_key;
}

template<typename K, typename V>
bool SST<K, V>::is_valid() const {
    return entry_count > 0 && !filename.empty();
}

#endif
