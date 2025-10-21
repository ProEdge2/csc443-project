#ifndef SST_CPP
#define SST_CPP

#include "sst.h"
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>

template<typename K, typename V>
SST<K, V>::SST(const std::string& file_path, BufferPool* bp)
    : filename(file_path), entry_count(0), buffer_pool(bp) {
}

template<typename K, typename V>
SST<K, V>::~SST() {
}

template<typename K, typename V>
bool SST<K, V>::create_from_memtable(const std::string& file_path, const std::vector<std::pair<K, V>>& sorted_data) {
    std::ofstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    entry_count = sorted_data.size();
    if (entry_count == 0) {
        return true;
    }

    min_key = sorted_data[0].first;
    max_key = sorted_data[entry_count - 1].first;
    filename = file_path;

    for (const auto& pair : sorted_data) {
        file.write(reinterpret_cast<const char*>(&pair.first), sizeof(K));
        file.write(reinterpret_cast<const char*>(&pair.second), sizeof(V));
    }

    file.close();
    return true;
}


template<typename K, typename V>
bool SST<K, V>::get(const K& key, V& value) const {
    if (entry_count == 0 || key < min_key || key > max_key) {
        return false;
    }

    return binary_search_file(key, value);
}

template<typename K, typename V>
std::vector<std::pair<K, V>> SST<K, V>::scan(const K& start_key, const K& end_key) const {
    std::vector<std::pair<K, V>> results;

    if (entry_count == 0 || start_key > max_key || end_key < min_key) {
        return results;
    }

    try {
        // Find the starting position using binary search
        size_t start_pos = binary_search_start_position(start_key);
        size_t entry_size = sizeof(K) + sizeof(V);

        // Read from start position until we find a key > end_key
        for (size_t i = start_pos; i < entry_count; i++) {
            size_t byte_offset = i * entry_size;
            size_t page_num = byte_offset / PAGE_SIZE;
            size_t offset_in_page = byte_offset % PAGE_SIZE;

            char page_data[PAGE_SIZE];
            PageID pid(filename, page_num);

            // Check buffer pool first
            bool page_loaded = false;
            if (buffer_pool && buffer_pool->get_page(pid, page_data)) {
                page_loaded = true;
            } else {
                // Miss - read from disk
                if (!read_page_from_disk(page_num, page_data)) {
                    break;
                }
                if (buffer_pool) {
                    buffer_pool->put_page(pid, page_data);
                }
                page_loaded = true;
            }

            if (!page_loaded) {
                break;
            }

            // Extract K, V from page data
            K key;
            V value;

            
            std::memcpy(&key, &page_data[offset_in_page], sizeof(K));
            std::memcpy(&value, &page_data[offset_in_page + sizeof(K)], sizeof(V));

            if (key > end_key) {
                break; // We've gone past the end of our range
            }

            if (key >= start_key) {
                results.push_back(std::make_pair(key, value));
            }
        }

    } catch (const std::exception& e) {
        std::cerr << "Error scanning SST file: " << e.what() << std::endl;
    }

    return results;
}

template<typename K, typename V>
bool SST<K, V>::binary_search_file(const K& target_key, V& value) const {
    size_t left = 0;
    size_t right = entry_count;
    size_t entry_size = sizeof(K) + sizeof(V);

    while (left < right) {
        size_t mid = left + (right - left) / 2;
        size_t byte_offset = mid * entry_size;
        size_t page_num = byte_offset / PAGE_SIZE;
        size_t offset_in_page = byte_offset % PAGE_SIZE;

        char page_data[PAGE_SIZE];
        PageID pid(filename, page_num);

        // Check buffer pool first
        bool page_loaded = false;
        if (buffer_pool && buffer_pool->get_page(pid, page_data)) {
            page_loaded = true;
        } else {
            // Miss - read from disk
            if (!read_page_from_disk(page_num, page_data)) {
                return false;
            }
            if (buffer_pool) {
                buffer_pool->put_page(pid, page_data);
            }
            page_loaded = true;
        }

        if (!page_loaded) {
            return false;
        }

        // Extract K, V from page data
        K key;
        V val;

        std::memcpy(&key, &page_data[offset_in_page], sizeof(K));
        std::memcpy(&val, &page_data[offset_in_page + sizeof(K)], sizeof(V));

        if (key == target_key) {
            value = val;
            return true;
        } else if (key < target_key) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }

    return false;
}

template<typename K, typename V>
size_t SST<K, V>::binary_search_start_position(const K& start_key) const {
    size_t left = 0;
    size_t right = entry_count;
    size_t result = entry_count; // Default to end if not found
    size_t entry_size = sizeof(K) + sizeof(V);

    while (left < right) {
        size_t mid = left + (right - left) / 2;
        size_t byte_offset = mid * entry_size;
        size_t page_num = byte_offset / PAGE_SIZE;
        size_t offset_in_page = byte_offset % PAGE_SIZE;

        char page_data[PAGE_SIZE];
        PageID pid(filename, page_num);

        // Check buffer pool first
        bool page_loaded = false;
        if (buffer_pool && buffer_pool->get_page(pid, page_data)) {
            page_loaded = true;
        } else {
            // Miss - read from disk
            if (!read_page_from_disk(page_num, page_data)) {
                return result;
            }
            if (buffer_pool) {
                buffer_pool->put_page(pid, page_data);
            }
            page_loaded = true;
        }

        if (!page_loaded) {
            return result;
        }

        // Extract key from page data
        K key;

        // Entry fits in single page (integers only - 8 bytes total)
        std::memcpy(&key, &page_data[offset_in_page], sizeof(K));

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
                                 std::unique_ptr<SST<K, V>>& sst_ptr,
                                 BufferPool* buffer_pool) {
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    // Create a new SST object to populate entry_count, min_key, max_key
    sst_ptr = std::make_unique<SST<K, V>>(file_path, buffer_pool);

    // Get file size
    file.seekg(0, std::ios::end);
    std::streampos file_size = file.tellg();
    file.seekg(0, std::ios::beg);

    // Each entry is K + V bytes
    size_t entry_count = static_cast<size_t>(file_size) / (sizeof(K) + sizeof(V));
    sst_ptr->entry_count = entry_count;

    if (entry_count == 0) {
        file.close();
        return true; // Empty
    }

    // Read first key
    K first_key;
    file.read(reinterpret_cast<char*>(&first_key), sizeof(K));
    sst_ptr->min_key = first_key;

    // Read last key
    file.seekg((entry_count - 1) * (sizeof(K) + sizeof(V)));
    K last_key;
    file.read(reinterpret_cast<char*>(&last_key), sizeof(K));
    sst_ptr->max_key = last_key;

    file.close();
    return true;
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

// Page I/O helper methods
template<typename K, typename V>
bool SST<K, V>::read_page_from_disk(size_t page_offset, char* page_data) const {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    file.seekg(page_offset * PAGE_SIZE);
    file.read(page_data, PAGE_SIZE);

    // Handle partial reads at EOF gracefully
    if (static_cast<size_t>(file.gcount()) < PAGE_SIZE) {
        // Zero out the rest of the page
        std::memset(page_data + file.gcount(), 0, PAGE_SIZE - static_cast<size_t>(file.gcount()));
    }

    file.close();
    return true;
}

template<typename K, typename V>
bool SST<K, V>::write_page_to_disk(size_t page_offset, const char* page_data) const {
    std::fstream file(filename, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        return false;
    }

    file.seekp(page_offset * PAGE_SIZE);
    file.write(page_data, PAGE_SIZE);
    file.close();
    return true;
}

template<typename K, typename V>
bool SST<K, V>::write_page_to_file(const std::string& filename,
                                   size_t page_offset,
                                   const char* page_data) {
    std::fstream file(filename, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        return false;
    }

    file.seekp(page_offset * PAGE_SIZE);
    file.write(page_data, PAGE_SIZE);
    file.close();
    return true;
}


#endif
