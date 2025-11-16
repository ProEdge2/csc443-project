#ifndef SST_CPP
#define SST_CPP

#include "sst.h"
#include <iostream>
#include <algorithm>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>

template<typename K, typename V>
SST<K, V>::SST(const std::string& file_path, BufferPool* bp, size_t sst_level, double false_positive_rate)
    : filename(file_path), entry_count(0), buffer_pool(bp), level(sst_level), bloom_filter_fpr(false_positive_rate) {
    bloom_filter = nullptr;
}

template<typename K, typename V>
SST<K, V>::~SST() {
}

template<typename K, typename V>
bool SST<K, V>::create_from_memtable(const std::string& file_path, const std::vector<std::pair<K, V>>& sorted_data, size_t sst_level) {
    entry_count = sorted_data.size();
    if (entry_count == 0) {
        return true;
    }

    min_key = sorted_data[0].first;
    max_key = sorted_data[entry_count - 1].first;
    filename = file_path;

    // Initialize Bloom Filter
    bloom_filter = std::make_unique<BloomFilter<K>>(entry_count, bloom_filter_fpr);

    size_t data_index = 0;
    size_t current_offset = sizeof(SSTHeader);
    leaf_start_offset = current_offset;
    std::vector<size_t> leaf_node_offsets;
    std::vector<K> leaf_separator_keys;

    // Create the b-tree from bottom up
    // leaf nodes
    while (data_index < entry_count) {
        auto leaf_node = std::make_unique<LeafNode<K, V>>();
        leaf_node->is_leaf = true;
        leaf_node->count = 0;

        // Create leaf nodes with key-value pairs (each node stores a page worth of <K, V> pairs)
        while (leaf_node->count < LeafNode<K,V>::PAIRS_COUNT && data_index < entry_count) {
            leaf_node->pairs[leaf_node->count] = sorted_data[data_index];
            // add leaf node keys to filter
            bloom_filter->add(sorted_data[data_index].first);
            leaf_node->count++;
            data_index++;
        }

        // Store largest key of leaf node
        leaf_separator_keys.push_back(leaf_node->pairs[leaf_node->count - 1].first);

        // Write leaf node to file
        if (!write_page_to_disk(current_offset, reinterpret_cast<char*>(leaf_node.get()))) {
            return false;
        }
        leaf_node_offsets.push_back(current_offset);

        current_offset += PAGE_SIZE;
    }

    // Create internal nodes
    std::vector<size_t> current_level_nodes = leaf_node_offsets;
    std::vector<K> current_level_keys = leaf_separator_keys;
    size_t internal_node_offset = current_offset;

    // iterate until we have one node left
    while (current_level_nodes.size() > 1) {
        std::vector<size_t> next_level_nodes;
        std::vector<K> next_level_keys;

        for (size_t i = 0; i < current_level_nodes.size(); ) {
            auto internal_node = std::make_unique<InternalNode<K>>();
            internal_node->is_leaf = false;
            internal_node->count = 0;

            // Iterate over (each node holds page size worth of leaf node pointers)
            for (size_t j = 0; j < InternalNode<K>::MAX_KEYS && i < current_level_nodes.size(); j++) {
                internal_node->keys[j] = current_level_keys[i];

                internal_node->children[j] = current_level_nodes[i];
                internal_node->count++;
                i++;
            }
            // Store largest key of new internal node for next level
            next_level_keys.push_back(internal_node->keys[internal_node->count - 1]);

            // Write internal node to file
            if (!write_page_to_disk(internal_node_offset, reinterpret_cast<char*>(internal_node.get()))) {
                return false;
            }
            next_level_nodes.push_back(internal_node_offset);
            internal_node_offset += PAGE_SIZE;
        }
        current_level_nodes = next_level_nodes;
        current_level_keys = next_level_keys;
    }

    // root node (last remaining node)
    root_page_offset = current_level_nodes.empty() ? 0 : current_level_nodes[0];

    // Bloom filter begins after all internal nodes
    current_offset = internal_node_offset;
    std::vector<char> bloom_filter_data;
    size_t padded_bloom_filter_size = 0;
    if (bloom_filter) {
        // Calculate number of bytes needed for bit_array
        size_t num_bits = bloom_filter->num_bits;
        size_t num_bytes = (num_bits + 7) / 8;

        // Round up to the nearest multiple of PAGE_SIZE
        padded_bloom_filter_size = ((num_bytes + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
        bloom_filter_data.resize(padded_bloom_filter_size);

        // Manually serialize into bytes
        for (size_t i = 0; i < num_bits; ++i) {
            if (bloom_filter->bit_array[i]) {
                bloom_filter_data[i / 8] |= (1 << (i % 8));
            }
        }
    }

    size_t bloom_filter_offset = current_offset;
    size_t bloom_filter_size = padded_bloom_filter_size;

    if (!bloom_filter_data.empty()) {
        // Write bloom filter data to file
        if (!write_page_to_disk(bloom_filter_offset, bloom_filter_data.data(), bloom_filter_size)) {
            return false;
        }
    }

    SSTHeader header;
    header.root_page_offset = root_page_offset;
    header.leaf_start_offset = leaf_start_offset;
    header.entry_count = entry_count;
    header.level = sst_level;
    header.false_positive_rate = bloom_filter_fpr;
    header.bloom_filter_offset = bloom_filter_offset;
    header.bloom_filter_size = bloom_filter_size; // Store the padded size
    header.bloom_filter_num_hash_functions = bloom_filter->num_hash_functions;
    header.bloom_filter_num_bits = bloom_filter->num_bits;

    // Write header to file
    if (!write_page_to_disk(0, reinterpret_cast<char*>(&header), sizeof(SSTHeader))) {
        return false;
    }

    return true;
}

template<typename K, typename V>
bool SST<K, V>::create_from_merge(const std::string& file_path,
                                 SST<K, V>* sst1,
                                  SST<K, V>* sst2,
                                  size_t target_level,
                                  std::unique_ptr<SST<K, V>>& result_sst) {
    constexpr size_t BUFFER_SIZE = 1024;


    std::vector<std::pair<K, V>> input_buffer1;
    std::vector<std::pair<K, V>> input_buffer2;
    std::vector<std::pair<K, V>> output_buffer;
    output_buffer.reserve(BUFFER_SIZE * 2);

    std::vector<std::pair<K, V>> all_merged_data;

    K min_key1 = sst1->get_min_key();
    K max_key1 = sst1->get_max_key();
    K min_key2 = sst2->get_min_key();
    K max_key2 = sst2->get_max_key();

    auto data1 = sst1->scan(min_key1, max_key1, SearchMode::B_TREE_SEARCH);
    auto data2 = sst2->scan(min_key2, max_key2, SearchMode::B_TREE_SEARCH);

    size_t idx1 = 0, idx2 = 0;

    while (idx1 < data1.size() || idx2 < data2.size()) {
        if (idx1 >= data1.size()) {
            all_merged_data.push_back(data2[idx2++]);
        } else if (idx2 >= data2.size()) {
            all_merged_data.push_back(data1[idx1++]);
        } else {
            if (data1[idx1].first < data2[idx2].first) {
                all_merged_data.push_back(data1[idx1++]);

            } else if (data1[idx1].first > data2[idx2].first) {
                all_merged_data.push_back(data2[idx2++]);

            } else {
                all_merged_data.push_back(data2[idx2]);
                idx1++;
                idx2++;
            }
        }
    }

    result_sst = std::make_unique<SST<K, V>>(file_path, nullptr, target_level);

    return result_sst->create_from_memtable(file_path, all_merged_data, target_level);
}


template<typename K, typename V>
bool SST<K, V>::get(const K& key, V& value, SearchMode mode) const {
    if (entry_count == 0 || key < min_key || key > max_key) {
        return false;
    }

    if (mode == SearchMode::B_TREE_SEARCH) {
        return b_tree_search(key, value);
    }
    else {
        return binary_search_file(key, value);
    }
}

template<typename K, typename V>
std::vector<std::pair<K, V>> SST<K, V>::scan(const K& start_key, const K& end_key, SearchMode mode) const {
    std::vector<std::pair<K, V>> results;

    if (entry_count == 0 || start_key > max_key || end_key < min_key) {
        return results;
    }

    SSTHeader header;
    char header_data[PAGE_SIZE];
    // load page (first check buffer pool, then check disk)
    if (!get_page_from_source(0, header_data)) {
        return results;
    }
    header = *reinterpret_cast<SSTHeader*>(header_data);

    size_t pairs_per_leaf = LeafNode<K,V>::PAIRS_COUNT;
    size_t num_leaf_nodes = (header.entry_count + pairs_per_leaf - 1) / pairs_per_leaf;
    size_t current_leaf_offset = 0;

    // Find starting leaf with B-Tree search
    if (mode == SearchMode::B_TREE_SEARCH) {
        // find the start key
        // if the start key is smaller than any key, start from the first leaf node
        current_leaf_offset = find_leaf_node(start_key);
        if (current_leaf_offset == 0) {
            current_leaf_offset = header.leaf_start_offset;
        }
    }

    // Find starting leaf with binary search
    else {
        size_t left = 0;
        size_t right = num_leaf_nodes;
        size_t start_leaf_index = 0;

        while (left < right) {
            size_t mid = left + (right - left) / 2;
            size_t page_offset = header.leaf_start_offset + (mid * PAGE_SIZE);

            char page_data[PAGE_SIZE];
            if (!get_page_from_source(page_offset, page_data)) {
                return results;
            }
            LeafNode<K, V>* leaf_node = reinterpret_cast<LeafNode<K, V>*>(page_data);

            if (leaf_node->pairs[leaf_node->count - 1].first < start_key) {
                left = mid + 1;
            }
            else {
                right = mid;
            }
        }
        start_leaf_index = left;
        current_leaf_offset = header.leaf_start_offset + (start_leaf_index * PAGE_SIZE);
    }

    // start scanning
    size_t current_leaf_index = (current_leaf_offset - header.leaf_start_offset) / PAGE_SIZE;
    bool first_leaf_processed = false;

    // scan through leaves
    while (current_leaf_index < num_leaf_nodes) {
        char page_data[PAGE_SIZE];
        if (!get_page_from_source(current_leaf_offset, page_data)) {
            break;
        }
        LeafNode<K, V>* leaf_node = reinterpret_cast<LeafNode<K, V>*>(page_data);

        size_t start_pos_in_leaf = 0;
        // binary search to find starting position in leaf (only for first leaf)
        if (!first_leaf_processed) {
            size_t left = 0;
            size_t right = leaf_node->count;
            while (left < right) {
                size_t mid = left + (right - left) / 2;
                if (leaf_node->pairs[mid].first < start_key) {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            }
            start_pos_in_leaf = left;
            first_leaf_processed = true;
        }
        for (size_t i = start_pos_in_leaf; i < leaf_node->count; i++) {
            if (leaf_node->pairs[i].first <= end_key) {
                results.push_back(leaf_node->pairs[i]);
            }
            else {
                // finished scan
                return results;
            }
        }

        // move to next leaf
        current_leaf_index++;
        current_leaf_offset = header.leaf_start_offset + (current_leaf_index * PAGE_SIZE);
    }
    return results;
}

template<typename K, typename V>
bool SST<K, V>::b_tree_search(const K& key, V& value) const {
    size_t leaf_node_offset = find_leaf_node(key);
    if (leaf_node_offset == 0) {
        return false;
    }

    char page_data[PAGE_SIZE];
    if (!get_page_from_source(leaf_node_offset, page_data)) {
        return false;
    }
    LeafNode<K, V>* leaf_node = reinterpret_cast<LeafNode<K, V>*>(page_data);

    // binary search in leaf node
    size_t left = 0;
    size_t right = leaf_node->count;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (leaf_node->pairs[mid].first < key) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }

    if (left < leaf_node->count && leaf_node->pairs[left].first == key) {
        value = leaf_node->pairs[left].second;
        return true;
    }

    // not found
    return false;
}

template<typename K, typename V>
size_t SST<K, V>::find_leaf_node(const K& key) const {
    SSTHeader header;
    char header_data[PAGE_SIZE];
    if (!get_page_from_source(0, header_data)) {
        return -1;
    }
    header = *reinterpret_cast<SSTHeader*>(header_data);

    char page_data[PAGE_SIZE];
    if (!get_page_from_source(root_page_offset, page_data)) {
        return -1;
    }
    // root node
    BTreeNode* current_node = reinterpret_cast<BTreeNode*>(page_data);

    size_t current_offset = root_page_offset;

    // while the current node is internal, binary search to find pointer to correct leaf within internal node
    while (!current_node->is_leaf) {
        InternalNode<K>* internal_node = reinterpret_cast<InternalNode<K>*>(current_node);

        // binary search to find first value equal or greater than key
        size_t left = 0;
        size_t right = internal_node->count;
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            if (internal_node->keys[mid] < key) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        size_t i = left;

        current_offset = internal_node->children[i];
        if (!get_page_from_source(current_offset, page_data)) {
            return -1;
        }
        current_node = reinterpret_cast<BTreeNode*>(page_data);
    }

    return current_offset;
}

template<typename K, typename V>
bool SST<K, V>::binary_search_file(const K& target_key, V& value) const {
    SSTHeader header;
    char header_data[PAGE_SIZE];
    if (!get_page_from_source(0, header_data)) {
        return false;
    }
    header = *reinterpret_cast<SSTHeader*>(header_data);

    size_t pairs_per_leaf = LeafNode<K,V>::PAIRS_COUNT;
    size_t num_leaf_nodes = (header.entry_count + pairs_per_leaf - 1) / pairs_per_leaf;

    size_t left = 0;
    size_t right = num_leaf_nodes;

    while (left < right) {
        size_t mid = left + (right - left) / 2;
        size_t page_offset = header.leaf_start_offset + (mid * PAGE_SIZE);

        char page_data[PAGE_SIZE];
        if (!get_page_from_source(page_offset, page_data)) {
            return false;
        }
        LeafNode<K, V>* leaf_node = reinterpret_cast<LeafNode<K, V>*>(page_data);

        // last key in leaf less than target
        if (leaf_node->pairs[leaf_node->count - 1].first < target_key) {
            left = mid + 1;
        }
        else {
            right = mid;
        }
    }

    if (left < num_leaf_nodes) {
        size_t page_offset = header.leaf_start_offset + (left * PAGE_SIZE);
        char page_data[PAGE_SIZE];
        if (!get_page_from_source(page_offset, page_data)) {
            return false;
        }
        LeafNode<K, V>* leaf_node = reinterpret_cast<LeafNode<K, V>*>(page_data);

        // binary search within leaf node
        size_t l = 0;
        size_t r = leaf_node->count;
        while (l < r) {
            size_t m = l + (r - l) / 2;
            if (leaf_node->pairs[m].first < target_key) {
                l = m + 1;
            }
            else {
                r = m;
            }
        }

        if (l < leaf_node->count && leaf_node->pairs[l].first == target_key) {
            value = leaf_node->pairs[l].second;
            return true;
        }
    }

    return false;
}

template<typename K, typename V>
bool SST<K, V>::load_existing_sst(const std::string& file_path,
                                 std::unique_ptr<SST<K, V>>& sst_ptr,
                                 BufferPool* bp) {
    SSTHeader header;
    char header_data[PAGE_SIZE];
    SST<K, V> sst(file_path, bp);
    if (!sst.get_page_from_source(0, header_data)) {
        // failed to load header
        return false;
    }
    header = *reinterpret_cast<SSTHeader*>(header_data);

    // Create a new SST object to populate header data
    sst_ptr = std::make_unique<SST<K, V>>(file_path, bp, header.level, header.false_positive_rate);
    sst_ptr->entry_count = header.entry_count;
    sst_ptr->root_page_offset = header.root_page_offset;
    sst_ptr->leaf_start_offset = header.leaf_start_offset;

    // Load Bloom filter
    if (header.bloom_filter_size > 0) {
        std::vector<char> bloom_filter_data(header.bloom_filter_size);
        if (!sst.read_page_from_disk(header.bloom_filter_offset, bloom_filter_data.data(), header.bloom_filter_size)) {
            return false;
        }

        // Create new bloom filter object
        sst_ptr->bloom_filter = std::make_unique<BloomFilter<K>>(
            header.bloom_filter_num_bits,
            header.bloom_filter_num_hash_functions,
            bloom_filter_data
        );
    }

    // populate min_key
    if (header.entry_count > 0) {
        char first_leaf_data[PAGE_SIZE];
        if (sst.get_page_from_source(header.leaf_start_offset, first_leaf_data)) {
            LeafNode<K, V>* first_leaf = reinterpret_cast<LeafNode<K, V>*>(first_leaf_data);
            if (first_leaf->count > 0) {
                sst_ptr->min_key = first_leaf->pairs[0].first;
            }
        }
    }

    // populate last_key
    size_t pairs_per_leaf = LeafNode<K,V>::PAIRS_COUNT;
    size_t num_leaf_nodes = (header.entry_count + pairs_per_leaf - 1) / pairs_per_leaf;
    size_t last_leaf_offset = header.leaf_start_offset + ((num_leaf_nodes - 1) * PAGE_SIZE);

    char last_leaf_data[PAGE_SIZE];
    if (sst.get_page_from_source(last_leaf_offset, last_leaf_data)) {
        LeafNode<K, V>* last_leaf = reinterpret_cast<LeafNode<K, V>*>(last_leaf_data);
        if (last_leaf->count > 0) {
            sst_ptr->max_key = last_leaf->pairs[last_leaf->count - 1].first;
        }
    }

    return true;
}

// scans buffer pool for page, then scans disk
template<typename K, typename V>
bool SST<K, V>::get_page_from_source(size_t page_offset, char* page_data) const {
    if (buffer_pool && buffer_pool->get_page({filename, page_offset}, page_data)) {
        return true;
    } else {
        // Miss - read from disk
        if (!read_page_from_disk(page_offset, page_data)) {
            return false;
        }
        if (buffer_pool) {
            buffer_pool->put_page({filename, page_offset}, page_data);
        }
        return true;
    }
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
size_t SST<K, V>::get_level() const {
    return level;
}

template<typename K, typename V>
bool SST<K, V>::bloom_filter_contains(const K& key) const {
    if (bloom_filter) {
        return bloom_filter->contains(key);
    }
    return false;
}

template<typename K, typename V>
bool SST<K, V>::is_valid() const {
    return entry_count > 0 && !filename.empty();
}

// Page I/O helper methods
template<typename K, typename V>
bool SST<K, V>::read_page_from_disk(size_t page_offset, char* page_data, size_t bytes_to_read) const {
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        return false;
    }

    file.seekg(page_offset);
    file.read(page_data, bytes_to_read);

    // Handle partial reads at EOF gracefully
    if (static_cast<size_t>(file.gcount()) < bytes_to_read) {
        // Zero out the rest of the page
        std::memset(page_data + file.gcount(), 0, bytes_to_read - static_cast<size_t>(file.gcount()));
    }

    file.close();
    return true;
}

template<typename K, typename V>
bool SST<K, V>::write_page_to_disk(size_t page_offset, const char* page_data, size_t bytes_to_write) const {
    std::fstream file(filename, std::ios::binary | std::ios::in | std::ios::out);
    if (!file.is_open()) {
        file.clear();
        // Create file
        file.open(filename, std::ios::binary | std::ios::out);
        if (!file.is_open()) {
            // Failed to create
            return false;
        }
        file.close(); // Close and reopen in read/write mode
        file.open(filename, std::ios::binary | std::ios::in | std::ios::out);
        if (!file.is_open()) {
            return false;
        }
    }

    file.seekp(page_offset);
    file.write(page_data, bytes_to_write);
    if (file.fail()) {
        file.close();
        return false;
    }

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
