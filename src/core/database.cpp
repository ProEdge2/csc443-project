#ifndef DATABASE_CPP
#define DATABASE_CPP

#include "database.h"
#include "../storage/sst.h"
#include <iostream>
#include <filesystem>
#include <algorithm>
#include <chrono>

template<typename K, typename V>
Database<K, V>::Database(const std::string& name, size_t memtable_max_size)
    : db_name(name), memtable_size(memtable_max_size), is_open(false) {
    db_directory = "data/" + db_name;
    current_memtable = nullptr;
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
        sst_files.clear();
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

    // TODO: ...
}

template<typename K, typename V>
bool Database<K, V>::get(const K& key, V& value) {
    if (!is_open) {
        return false;
    }

    // TODO: ...
}

template<typename K, typename V>
std::vector<std::pair<K, V>> Database<K, V>::scan(const K& start_key, const K& end_key) {
    std::vector<std::pair<K, V>> results;

    if (!is_open) {
        return results;
    }


    // TODO: ...
}

template<typename K, typename V>
void Database<K, V>::flush_memtable_to_sst() {
    if (!current_memtable || current_memtable->size() == 0) {
        return;
    }

    // TODO: ...
}

template<typename K, typename V>
void Database<K, V>::load_existing_ssts() {
    if (!std::filesystem::exists(db_directory)) {
        return;
    }

    // TODO: ...
}

template<typename K, typename V>
std::string Database<K, V>::generate_sst_filename() {
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();

    return "sst_" + std::to_string(timestamp) + ".sst";
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
