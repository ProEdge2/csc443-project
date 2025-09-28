#ifndef SST_H
#define SST_H

#include <string>
#include <vector>
#include <fstream>
#include <utility>

template<typename K, typename V>
class SST {
private:
    std::string filename;
    size_t entry_count;
    K min_key;
    K max_key;

    struct SSTEntry {
        K key;
        V value;

        SSTEntry() = default;
        SSTEntry(const K& k, const V& v) : key(k), value(v) {}
    };

    bool binary_search_file(const K& target_key, V& value) const;

public:
    SST(const std::string& file_path);
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
                                  std::unique_ptr<SST<K, V>>& sst_ptr);
};

#include "sst.cpp"

#endif
