#ifndef SST_H
#define SST_H

#include <string>
#include <vector>
#include <fstream>
#include <utility>
#include "../buffer/buffer_pool.h"

class BufferPool;

template<typename K, typename V>
class SST {
private:
    std::string filename;
    size_t entry_count;
    K min_key;
    K max_key;
    BufferPool* buffer_pool;

    struct SSTEntry {
        K key;
        V value;

        SSTEntry() = default;
        SSTEntry(const K& k, const V& v) : key(k), value(v) {}
    };

    bool binary_search_file(const K& target_key, V& value) const;
    size_t binary_search_start_position(const K& start_key) const;

    // Page I/O helpers
    bool read_page_from_disk(size_t page_offset, char* page_data) const;
    bool write_page_to_disk(size_t page_offset, const char* page_data) const;


public:
    SST(const std::string& file_path, BufferPool* bp = nullptr);
    ~SST();

    bool create_from_memtable(const std::string& file_path,
                             const std::vector<std::pair<K, V>>& sorted_data);

    bool get(const K& key, V& value) const;
    std::vector<std::pair<K, V>> scan(const K& start_key, const K& end_key) const;

    const std::string& get_filename() const;
    size_t get_entry_count() const;
    const K& get_min_key() const;
    const K& get_max_key() const;

    bool is_valid() const;

    static bool load_existing_sst(const std::string& file_path,
                                  std::unique_ptr<SST<K, V>>& sst_ptr,
                                  BufferPool* buffer_pool = nullptr);

    // Static helper for write-back callback
    static bool write_page_to_file(const std::string& filename,
                                   size_t page_offset,
                                   const char* page_data);
};

#include "sst.cpp"

#endif
