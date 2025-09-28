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

    // TODO: ...
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

    // TODO: ...
}

template<typename K, typename V>
bool SST<K, V>::binary_search_file(const K& target_key, V& value) const {
    // TODO: ...
}

template<typename K, typename V>
bool SST<K, V>::load_existing_sst(const std::string& file_path,
                                 std::unique_ptr<SST<K, V>>& sst_ptr) {
    // TODO: ...
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
