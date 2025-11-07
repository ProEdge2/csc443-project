#ifndef DATABASE_CPP
#define DATABASE_CPP

#include "database.h"
#include "../storage/sst.h"
#include <iostream>
#include <filesystem>
#include <algorithm>
#include <chrono>
#include <map>

template<typename K, typename V>
Database<K, V>::Database(const std::string& name, size_t memtable_max_size)
    : db_name(name), memtable_size(memtable_max_size), is_open(false) {
    db_directory = "data/" + db_name;
    current_memtable = nullptr;

    // Initialize buffer pool with write-back callback
    auto write_back_cb = [](const PageID& pid, const char* data) {
        // Write dirty page back to disk
        SST<K,V>::write_page_to_file(pid.filename, pid.offset, data);
    };

    buffer_pool = std::make_unique<BufferPool>(
        2,      // initial_depth
        10,     // max_depth
        4,      // pages_per_bucket
        128,    // max_pages (512KB)
        true,   // enable_eviction
        write_back_cb,
        10      // flooding_threshold_pages (default)
    );
}

template<typename K, typename V>
Database<K, V>::~Database() {
    if (is_open) {
        close();
    }
}

template<typename K, typename V>
bool Database<K, V>::open() {
    if (is_open) {
        return false;
    }

    try {
        ensure_directory_exists();
        current_memtable = std::make_unique<RedBlackTree<K, V>>(memtable_size);
        load_existing_ssts();
        is_open = true;

        std::cout << "Database '" << db_name << "' opened successfully." << std::endl;
        std::cout << "Found " << get_sst_count() << " existing SST files acros " << levels.size() << " levels." << std::endl;

        return true;
    } catch (const std::exception& e) {
        std::cerr << "Failed to open database: " << e.what() << std::endl;
        return false;
    }
}

template<typename K, typename V>
bool Database<K, V>::close() {
    if (!is_open) {
        return false;
    }

    try {
        if (current_memtable && current_memtable->size() > 0) {
            flush_memtable_to_sst();
        }

        current_memtable.reset();
        // Don't clear levels - keep them in memory for persistence
        is_open = false;

        std::cout << "Database '" << db_name << "' closed successfully" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error closing database: " << e.what() << std::endl;
        return false;
    }
}

template<typename K, typename V>
bool Database<K, V>::put(const K& key, const V& value) {
    if (!is_open || !current_memtable) {
        return false;
    }

    // Try to insert into memtable
    if (current_memtable->put(key, value)) {
        return true;
    }

    // If memtable is full, flush it to SST and try again
    if (current_memtable->is_full()) {
        flush_memtable_to_sst();
        return current_memtable->put(key, value);
    }

    return false;
}

template<typename K, typename V>
bool Database<K, V>::remove(const K& key) {
    return put(key, TOMBSTONE);
}

template<typename K, typename V>
bool Database<K, V>::get(const K& key, V& value, SearchMode mode) {
    if (!is_open || !current_memtable) {
        return false;
    }

    // Search in memtable first (youngest)
    if (current_memtable && current_memtable->get(key, value)) {
        if (value == TOMBSTONE) {
            return false;
        }
        return true;
    }

    // Search levels from youngest to oldest (level 0 to higher levels)
    for (size_t level = 0; level < levels.size(); level++) {
        // Search SSTs in reverse order within each level (youngest first)
        for (auto it = levels[level].rbegin(); it != levels[level].rend(); ++it) {
            const auto& sst_ptr = *it;
            if (key < sst_ptr->get_min_key() || key > sst_ptr->get_max_key())
                continue;
            if (sst_ptr->get(key, value, mode)) {
                if (value == TOMBSTONE) {
                    return false;
                }
                return true;
            }
        }
    }

    return false;
}

template<typename K, typename V>
std::pair<K, V>* Database<K, V>::scan(const K& start_key, const K& end_key, size_t& result_size, SearchMode mode) {
    result_size = 0;

    if (!is_open) {
        return nullptr;
    }

    std::map<K, V> result_map;

    // Scan levels from oldest to youngest (higher levels first)
    for (int level = static_cast<int>(levels.size()) - 1; level >= 0; level--) {
        for (const auto& sst : levels[level]) {
            if (end_key < sst->get_min_key() || start_key > sst->get_max_key()) {
                continue;
            }
            auto sst_results = sst->scan(start_key, end_key, mode);
            for (const auto& pair : sst_results) {
                result_map[pair.first] = pair.second;
            }
        }
    }

    // Scan memtable last (youngest)
    if (current_memtable) {
        auto memtable_results = current_memtable->scan(start_key, end_key);
        for (const auto& pair : memtable_results) {
            result_map[pair.first] = pair.second;
        }
    }

    // take tombstones from results
    for (auto it = result_map.begin(); it != result_map.end(); ) {
        if (it->second == TOMBSTONE) {
            it = result_map.erase(it);
        } else {
            ++it;
        }
    }

    // Convert map to array
    result_size = result_map.size();
    if (result_size == 0) {
        return nullptr;
    }

    std::pair<K, V>* result_array = new std::pair<K, V>[result_size];
    size_t i = 0;
    for (const auto& pair : result_map) {
        result_array[i++] = pair;
    }
    return result_array;
}

template<typename K, typename V>
void Database<K, V>::flush_memtable_to_sst() {
    if (!current_memtable || current_memtable->size() == 0) {
        return;
    }

    try {
        if (levels.empty()) {
            levels.resize(1);
        }

        // filename for level 0
        std::string sst_filename = generate_sst_filename(0);
        std::string sst_path = db_directory + "/" + sst_filename;

        // Get all data from memtable using scan with min/max bounds
        std::vector<std::pair<K, V>> memtable_data;
        if (current_memtable->size() > 0) {
            K min_key = current_memtable->get_min_key();
            K max_key = current_memtable->get_max_key();
            memtable_data = current_memtable->scan(min_key, max_key);
        }

        // create SST file from memtable data at level 0
        auto sst = std::make_unique<SST<K, V>>(sst_path, buffer_pool.get(), 0);
        if (sst->create_from_memtable(sst_path, memtable_data, 0)) {
            levels[0].push_back(std::move(sst));
            std::cout << "Successfully flushed memtable to SST: " << sst_filename << std::endl;
        } else {
            std::cerr << "Create SST file fail: " << sst_filename << std::endl;
        }

        // Clear the memtable after successful flush
        current_memtable->clear();

        try_compaction();

    } catch (const std::exception& e) {
        std::cerr << "Error flushing memtable to SST: " << e.what() << std::endl;
    }
}

template<typename K, typename V>
void Database<K, V>::load_existing_ssts() {
    if (!std::filesystem::exists(db_directory)) {
        return;
    }

    levels.clear();

    std::map<size_t, std::vector<std::unique_ptr<SST<K, V>>>> ssts_by_level;

    for (const auto& entry : std::filesystem::directory_iterator(db_directory)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        std::string filename = entry.path().string();
        if (filename.find(".sst") == std::string::npos) {
            continue;
        }

        std::unique_ptr<SST<K, V>> sst;
        if (SST<K, V>::load_existing_sst(filename, sst, buffer_pool.get())) {
            size_t level = sst->get_level();
            ssts_by_level[level].push_back(std::move(sst));
        }
    }

    // Organize SSTs into levels
    if (!ssts_by_level.empty()) {
        size_t max_level = ssts_by_level.rbegin()->first;
        levels.resize(max_level + 1);

        for (auto& [level, sst_vec] : ssts_by_level) {
            levels[level] = std::move(sst_vec);
         }
    }
}

template<typename K, typename V>
std::string Database<K, V>::generate_sst_filename(size_t level) {
    static int counter = 0;
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::string filename;
    do {
        filename = "sst_L" + std::to_string(level) + "_" +
                std::to_string(timestamp) + "_" +
                std::to_string(counter++) + ".sst";
    } while (std::filesystem::exists(db_directory + "/" + filename));

    return filename;
}

template<typename K, typename V>
void Database<K, V>::try_compaction() {
    for (size_t level = 0; level < levels.size(); level++) {
        if (levels[level].size() >= 2) {
            compact_level(level);

        }
    }
}

template<typename K, typename V>
void Database<K, V>::compact_level(size_t level) {
    if (levels[level].size() < 2) {
        return;
    }

    auto sst1 = std::move(levels[level][0]);
    auto sst2 = std::move(levels[level][1]);

    // remove first two sst from the current level
    levels[level].erase(levels[level].begin(), levels[level].begin() + 2);

    // existence check
    size_t target_level = level + 1;
    while (levels.size() <= target_level) {
        levels.push_back({});
    }

    // new filename for merged sst
    std::string merged_filename = generate_sst_filename(target_level);
    std::string merged_path = db_directory + "/" + merged_filename;

    // merge logic
    std::unique_ptr<SST<K, V>> merged_sst;
    if (SST<K, V>::create_from_merge(merged_path, sst1.get(), sst2.get(), target_level, merged_sst)) {
        levels[target_level].push_back(std::move(merged_sst));
        std::cout << "Successfull compacted level " << level << " to level " << target_level << std::endl;

        // cleanup old SST files
        std::filesystem::remove(sst1->get_filename());
        std::filesystem::remove(sst2->get_filename());

        if (levels[target_level].size() >= 2) {
            compact_level(target_level);
        }
    } else {
        std::cerr << "Failed to compct level " << level << std::endl;
        // Put SSTs back if compaction failed
        levels[level].insert(levels[level].begin(), std::move(sst1));
        levels[level].insert(levels[level].begin() + 1, std::move(sst2));
    }
}

template<typename K, typename V>
void Database<K, V>::ensure_directory_exists() {
    if (!std::filesystem::exists(db_directory)) {
        std::filesystem::create_directories(db_directory);
    }
}

template<typename K, typename V>
bool Database<K, V>::is_database_open() const {
    return is_open;
}

template<typename K, typename V>
size_t Database<K, V>::get_sst_count() const {
    size_t count = 0;
    for (const auto& level : levels) {
        count += level.size();
    }

    return count;
}

template<typename K, typename V>
size_t Database<K, V>::get_memtable_size() const {
    return current_memtable ? current_memtable->size() : 0;
}

template<typename K, typename V>
void Database<K, V>::print_stats() const {
    std::cout << "\n=== Database Statistics ===" << std::endl;
    std::cout << "Name: " << db_name << std::endl;
    std::cout << "Status: " << (is_open ? "Open" : "Closed") << std::endl;
    std::cout << "Memtable size: " << get_memtable_size() << "/" << memtable_size << std::endl;
    std::cout << "SST files: " << get_sst_count() << std::endl;
    std::cout << "Directory: " << db_directory << std::endl;
}

#endif
