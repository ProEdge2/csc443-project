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
        write_back_cb
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
        std::cout << "Found " << sst_files.size() << " existing SST files." << std::endl;

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
        // Don't clear sst_files - keep them in memory for persistence
        is_open = false;

        std::cout << "Database '" << db_name << "' closed successfully." << std::endl;
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
bool Database<K, V>::get(const K& key, V& value) {
    if (!is_open || !current_memtable) {
        return false;
    }

    // Search in memtable first
    if (current_memtable && current_memtable->get(key, value)) {
        return true;
    }

    // Search SST files in reverse order (ensures youngest before oldest)
    for (auto it = sst_files.rbegin(); it != sst_files.rend(); ++it) {
        const auto& sst_ptr = *it;
        if (key < sst_ptr->get_min_key() || key > sst_ptr->get_max_key())
            continue;
        if (sst_ptr->get(key, value))
            return true;
    }

    return false; // not found
}

template<typename K, typename V>
std::pair<K, V>* Database<K, V>::scan(const K& start_key, const K& end_key, size_t& result_size) {
    result_size = 0;

    if (!is_open) {
        return nullptr;
    }

    std::map<K, V> result_map;

    // Scan SST files in reverse order (youngest first)
    for (auto it = sst_files.rbegin(); it != sst_files.rend(); ++it) {
        const auto& sst = *it;
        if (end_key < sst->get_min_key() || start_key > sst->get_max_key()) {
            continue;
        }
        auto sst_results = sst->scan(start_key, end_key);
        for (const auto& pair : sst_results) {
            if (result_map.find(pair.first) == result_map.end()) {
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
        // Generate SST filename
        std::string sst_filename = generate_sst_filename();
        std::string sst_path = db_directory + "/" + sst_filename;

        // Get all data from memtable using scan with min/max bounds
        std::vector<std::pair<K, V>> memtable_data;
        if (current_memtable->size() > 0) {
            K min_key = current_memtable->get_min_key();
            K max_key = current_memtable->get_max_key();
            memtable_data = current_memtable->scan(min_key, max_key);
        }

        // Create SST file from memtable data
        auto sst = std::make_unique<SST<K, V>>(sst_path, buffer_pool.get());
        if (sst->create_from_memtable(sst_path, memtable_data)) {
            sst_files.push_back(std::move(sst));
            std::cout << "Successfully flushed memtable to SST: " << sst_filename << std::endl;
        } else {
            std::cerr << "Failed to create SST file: " << sst_filename << std::endl;
        }

        // Clear the memtable after successful flush
        current_memtable->clear();

    } catch (const std::exception& e) {
        std::cerr << "Error flushing memtable to SST: " << e.what() << std::endl;
    }
}

template<typename K, typename V>
void Database<K, V>::load_existing_ssts() {
    if (!std::filesystem::exists(db_directory)) {
        return;
    }

    sst_files.clear();

    std::vector<std::pair<std::string, std::unique_ptr<SST<K, V>>>> sst_vector;

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
            sst_vector.push_back({filename, std::move(sst)});
        }
    }

    // Sort by timestamp, counter in filename (youngest last)
    std::sort(sst_vector.begin(), sst_vector.end(), [](const auto& a, const auto& b) {
        auto extract_ts_and_counter = [](const std::string& s) -> std::pair<long long, long long> {
            size_t pos1 = s.rfind("sst_");
            if (pos1 == std::string::npos) {
                return {-1LL, -1LL};
            }
            pos1 += 4;

            size_t underscore_pos = s.find("_", pos1);
            size_t dot_pos = s.find(".sst", pos1);

            if (dot_pos == std::string::npos) {
                return {-1LL, -1LL};
            }

            try {
                long long timestamp;
                long long counter = 0;

                if (underscore_pos != std::string::npos && underscore_pos < dot_pos) {
                    timestamp = std::stoll(s.substr(pos1, underscore_pos - pos1));
                    counter = std::stoll(s.substr(underscore_pos + 1, dot_pos - underscore_pos - 1));
                } else {
                    timestamp = std::stoll(s.substr(pos1, dot_pos - pos1));
                }

                return {timestamp, counter};
            }
            catch (...) {
                return {-1LL, -1LL};
            }
        };

        auto [ts_a, cnt_a] = extract_ts_and_counter(a.first);
        auto [ts_b, cnt_b] = extract_ts_and_counter(b.first);

        if (ts_a != ts_b) {
            return ts_a < ts_b;
        }
        return cnt_a < cnt_b;
    });

    // Move SSTs into sst_files vector (youngest last)
    for (auto& p : sst_vector) {
        sst_files.push_back(std::move(p.second));
    }
}

template<typename K, typename V>
std::string Database<K, V>::generate_sst_filename() {
    static int counter = 0;
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    std::string filename;
    do {
        filename = "sst_" + std::to_string(timestamp) + "_" + std::to_string(counter++) + ".sst";
    } while (std::filesystem::exists(db_directory + "/" + filename));

    return filename;
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
    return sst_files.size();
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
