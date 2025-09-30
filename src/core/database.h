#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <memory>
#include <filesystem>
#include "../memtable/memtable.h"

template<typename K, typename V>
class SST;

template<typename K, typename V>
class Database {
private:
    std::string db_name;
    std::string db_directory;
    size_t memtable_size;
    std::unique_ptr<RedBlackTree<K, V>> current_memtable;
    std::vector<std::unique_ptr<SST<K, V>>> sst_files;
    bool is_open;

    // TODO: add auto flushing when memtable reaches capacity
    void load_existing_ssts();
    std::string generate_sst_filename();
    void ensure_directory_exists();

public:
    Database(const std::string& name, size_t memtable_max_size = 1000);
    ~Database();

    bool open();
    bool close();

    bool put(const K& key, const V& value);

    bool get(const K& key, V& value);

    std::pair<K, V>* scan(const K& start_key, const K& end_key, size_t& result_size);

    bool is_database_open() const;
    size_t get_sst_count() const;
    size_t get_memtable_size() const;

    void print_stats() const;

    // Public method for testing SST flushing
    void flush_memtable_to_sst();
};

#include "database.cpp"

#endif
