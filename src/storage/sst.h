#ifndef SST_H
#define SST_H

#include <string>
#include <vector>
#include <fstream>
#include <utility>
#include "../buffer/buffer_pool.h"
#include "../filter/bloom_filter.h"

class BufferPool;

enum class SearchMode {
    B_TREE_SEARCH,
    BINARY_SEARCH
};

struct SSTHeader {
    size_t root_page_offset;
    size_t leaf_start_offset;
    size_t entry_count;
    size_t level;
    double false_positive_rate;
    size_t bloom_filter_offset;
    size_t bloom_filter_size;
    size_t bloom_filter_num_hash_functions;
    size_t bloom_filter_num_bits;
    char padding[PAGE_SIZE - (8 * sizeof(size_t) + sizeof(double))];
};

struct BTreeNode {
    bool is_leaf;
    size_t count;
};

template<typename K>
// internal nodes
struct InternalNode : public BTreeNode {
    static constexpr size_t DATA_SPACE = PAGE_SIZE - sizeof(BTreeNode);
    static constexpr size_t MAX_KEYS = (DATA_SPACE - sizeof(size_t)) / (sizeof(K) + sizeof(size_t));
    static constexpr size_t MAX_CHILDREN = MAX_KEYS;

    K keys[MAX_KEYS];
    size_t children[MAX_CHILDREN];
};

template<typename K, typename V>
// leaf nodes
struct LeafNode : public BTreeNode {
    static constexpr size_t DATA_SPACE = PAGE_SIZE - sizeof(BTreeNode);
    static constexpr size_t PAIRS_COUNT = DATA_SPACE / sizeof(std::pair<K, V>);
    std::pair<K, V> pairs[PAIRS_COUNT];
};

template<typename K, typename V>
class SST {
private:
    std::string filename;
    size_t entry_count;
    K min_key;
    K max_key;
    BufferPool* buffer_pool;
    size_t level;
    size_t root_page_offset;
    size_t leaf_start_offset;
    std::unique_ptr<BloomFilter<K>> bloom_filter;
    double bloom_filter_fpr;

    struct SSTEntry {
        K key;
        V value;

        SSTEntry() = default;
        SSTEntry(const K& k, const V& v) : key(k), value(v) {}
    };

    bool binary_search_file(const K& target_key, V& value) const;

    bool b_tree_search(const K& key, V& value) const;
    size_t find_leaf_node(const K& key) const;

    // Page I/O helpers
    bool read_page_from_disk(size_t page_offset, char* page_data, size_t bytes_to_read = PAGE_SIZE) const;
    bool write_page_to_disk(size_t page_offset, const char* page_data, size_t bytes_to_write = PAGE_SIZE) const;
    bool get_page_from_source(size_t page_offset, char* page_data) const;


public:
    SST(const std::string& file_path, BufferPool* bp = nullptr, size_t sst_level = 0, double false_positive_rate = 0.01);
    ~SST();

    bool create_from_memtable(const std::string& file_path,
                             const std::vector<std::pair<K, V>>& sorted_data,
                             size_t sst_level = 0);

    static bool create_from_merge(const std::string& file_path,
                                  SST<K, V>* sst1,
                                  SST<K, V>* sst2,
                                  size_t target_level,
                                  std::unique_ptr<SST<K, V>>& result_sst);

    bool get(const K& key, V& value, SearchMode mode) const;
    std::vector<std::pair<K, V>> scan(const K& start_key, const K& end_key, SearchMode mode) const;

    const std::string& get_filename() const;
    size_t get_entry_count() const;
    const K& get_min_key() const;
    const K& get_max_key() const;
    size_t get_level() const;

    bool is_valid() const;

    static bool load_existing_sst(const std::string& file_path,
                                  std::unique_ptr<SST<K, V>>& sst_ptr,
                                  BufferPool* buffer_pool = nullptr);

    bool bloom_filter_contains(const K& key) const;

    // Static helper for write-back callback
    static bool write_page_to_file(const std::string& filename,
                                   size_t page_offset,
                                   const char* page_data);
};

#include "sst.cpp"

#endif
