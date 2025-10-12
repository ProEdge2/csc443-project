#ifndef BUFFER_POOL_H
#define BUFFER_POOL_H

#include <string>
#include <vector>
#include <memory>
#include <cstring>
#include <functional>

constexpr size_t PAGE_SIZE = 4096;

struct PageID {
    std::string filename;
    size_t offset;

    PageID(const std::string& file, size_t off) : filename(file), offset(off) {}

    bool operator==(const PageID& other) const {
        return filename == other.filename && offset == other.offset;
    }

    bool operator!=(const PageID& other) const {
        return !(*this == other);
    }
};

struct Page {
    PageID page_id;
    char data[PAGE_SIZE];
    bool is_valid;

    Page(const PageID& id) : page_id(id), is_valid(false) {
        std::memset(data, 0, PAGE_SIZE);
    }

    Page(const PageID& id, const char* page_data) : page_id(id), is_valid(true) {
        std::memcpy(data, page_data, PAGE_SIZE);
    }
};

struct Bucket {
    std::vector<std::unique_ptr<Page>> pages;
    size_t local_depth;
    size_t max_bucket_size;

    Bucket(size_t depth, size_t max_size)
        : local_depth(depth), max_bucket_size(max_size) {}

    bool is_full() const {
        return pages.size() >= max_bucket_size;
    }

    bool contains(const PageID& page_id) const {
        for (const auto& page : pages) {
            if (page->page_id == page_id) {
                return page->is_valid;
            }
        }
        return false;
    }

    Page* find_page(const PageID& page_id) {
        for (auto& page : pages) {
            if (page->page_id == page_id) {
                return page.get();
            }
        }

        return nullptr;
    }


    bool remove_page(const PageID& page_id) {
        for (auto it = pages.begin(); it != pages.end(); ++it) {
             if ((*it)->page_id == page_id) {
                pages.erase(it);
                return true;
            }
        }

        return false;
    }
};

class BufferPool {
private:
    std::vector<std::shared_ptr<Bucket>> directory;
    size_t global_depth;
    size_t initial_depth;
    size_t max_global_depth;
    size_t pages_per_bucket;
    size_t current_page_count;
    size_t max_pages;

    size_t hash_page_id(const PageID& page_id) const;
    size_t xxhash(const char* data, size_t length, uint64_t seed = 0) const;

    size_t get_bucket_index(size_t hash_value) const;
    void split_bucket(size_t bucket_index);

    void double_directory();
    bool can_expand() const;

public:
    BufferPool(size_t initial_global_depth, size_t max_depth, size_t bucket_size, size_t max_page_limit);
    ~BufferPool();

    bool put_page(const PageID& page_id, const char* page_data);
    bool get_page(const PageID& page_id, char* page_data);
    bool contains_page(const PageID& page_id) const;
    bool remove_page(const PageID& page_id);

    size_t get_directory_size() const;
    size_t get_global_depth() const;
    size_t get_page_count() const;

    size_t get_max_pages() const;
    bool is_full() const;

    void clear();
    void print_stats() const;
};

#include "buffer_pool.cpp"

#endif
