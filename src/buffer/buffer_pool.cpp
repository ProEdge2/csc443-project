#ifndef BUFFER_POOL_CPP
#define BUFFER_POOL_CPP

#include "buffer_pool.h"
#include <iostream>
#include <cstring>
#include <cmath>
#include <set>

BufferPool::BufferPool(size_t initial_global_depth, size_t max_depth, size_t bucket_size, size_t max_page_limit,
                       bool enable_eviction,
                       std::function<void(const PageID&, const char*)> write_back_cb)
    : global_depth(initial_global_depth),
      initial_depth(initial_global_depth),
      max_global_depth(max_depth),
      pages_per_bucket(bucket_size),
      current_page_count(0),
      max_pages(max_page_limit) {

    size_t dir_size = 1 << global_depth;
    directory.resize(dir_size);

    for (size_t i = 0; i < dir_size; i++) {
        directory[i] = std::make_shared<Bucket>(global_depth, pages_per_bucket);
    }

    eviction_enabled = enable_eviction;
    write_back = write_back_cb;
}

BufferPool::~BufferPool() {
    clear();
}

// chose the xxhash approach
size_t BufferPool::xxhash(const char* data, size_t length, uint64_t seed) const {
    const uint64_t PRIME64_1 = 11400714785074694791ULL;
    const uint64_t PRIME64_2 = 14029467366897019727ULL;
    const uint64_t PRIME64_3 = 1609587929392839161ULL;
    const uint64_t PRIME64_4 = 9650029242287828579ULL;
    const uint64_t PRIME64_5 = 2870177450012600261ULL;

    // basic hash initializaiton
    uint64_t hash = seed + PRIME64_5 + static_cast<uint64_t>(length);

    const unsigned char* p = reinterpret_cast<const unsigned char*>(data);
    const unsigned char* end = p + length;


    // scramble the hash
    while (p + 8 <= end) {
        uint64_t k1;
        std::memcpy(&k1, p, sizeof(k1));
        k1 *= PRIME64_2;
        k1 = (k1 << 31) | (k1 >> (64 - 31));
        k1 *= PRIME64_1;
        hash ^= k1;
        hash = ((hash << 27) | (hash >> (64 - 27))) * PRIME64_1 + PRIME64_4;
        p += 8;
    }

    if (p + 4 <= end) {
        uint32_t k1;
        std::memcpy(&k1, p, sizeof(k1));
        hash ^= static_cast<uint64_t>(k1) * PRIME64_1;
        hash = ((hash << 23) | (hash >> (64 - 23))) * PRIME64_2 + PRIME64_3;
        p += 4;
    }

    while (p < end) {
        hash ^= static_cast<uint64_t>(*p) * PRIME64_5;
        hash = ((hash << 11) | (hash >> (64 - 11))) * PRIME64_1;
        p++;
    }

    // final scramble to affect all bits
    hash ^= hash >> 33;
    hash *= PRIME64_2;
    hash ^= hash >> 29;
    hash *= PRIME64_3;
    hash ^= hash >> 32;

    return static_cast<size_t>(hash);
}

size_t BufferPool::hash_page_id(const PageID& page_id) const {
    std::string combined = page_id.filename + std::to_string(page_id.offset);
    return xxhash(combined.c_str(), combined.length());
}

size_t BufferPool::get_bucket_index(size_t hash_value) const {
    size_t mask = (1 << global_depth) - 1;
    return hash_value & mask;
}

bool BufferPool::can_expand() const {
    return global_depth < max_global_depth;
}

void BufferPool::double_directory() {
    size_t old_size = directory.size();
    directory.resize(old_size * 2);

    for (size_t i = 0; i < old_size; i++) {
        directory[i + old_size] = directory[i];
    }

    global_depth++;
}

void BufferPool::split_bucket(size_t bucket_index) {
    auto old_bucket = directory[bucket_index];

    if (old_bucket->local_depth == global_depth && can_expand()) {
        double_directory();
    }

    if (!can_expand() && old_bucket->local_depth == global_depth) {
        return;
    }

    size_t new_local_depth = old_bucket->local_depth + 1;
    auto bucket0 = std::make_shared<Bucket>(new_local_depth, pages_per_bucket);
    auto bucket1 = std::make_shared<Bucket>(new_local_depth, pages_per_bucket);

    size_t mask = (1 << new_local_depth) - 1;
    size_t split_bit = 1 << old_bucket->local_depth;

    for (auto& page : old_bucket->pages) {
        size_t hash_val = hash_page_id(page->page_id);
        if ((hash_val & mask) & split_bit) {
            bucket1->pages.push_back(std::move(page));
        } else {
            bucket0->pages.push_back(std::move(page));
        }
    }

    for (size_t i = 0; i < directory.size(); i++) {
        if (directory[i] == old_bucket) {
            if ((i & mask) & split_bit) {
                directory[i] = bucket1;
            } else {
                directory[i] = bucket0;
            }
        }
    }
}

bool BufferPool::put_page(const PageID& page_id, const char* page_data) {
    if (page_data == nullptr) {
        return false;
    }

    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    auto bucket = directory[bucket_index];

    Page* existing_page = bucket->find_page(page_id);
    if (existing_page) {
        std::memcpy(existing_page->data, page_data, PAGE_SIZE);
        existing_page->is_valid = true;
        existing_page->reference_bit = true;
        return true;
    }

    if (current_page_count >= max_pages) {
        if (!eviction_enabled) {
            return false;
        }
        if (!evict_one()) {
            return false;
        }
    }

    while (bucket->is_full()) {
        if (!can_expand() && bucket->local_depth >= global_depth) {
            return false;
        }

        split_bucket(bucket_index);
        bucket_index = get_bucket_index(hash_val);
        bucket = directory[bucket_index];
    }

    auto new_page = std::make_unique<Page>(page_id, page_data);
    new_page->reference_bit = false;
    bucket->pages.push_back(std::move(new_page));
    // Track in clock ring
    Page* raw = bucket->pages.back().get();
    clock_ring.push_back(raw);
    current_page_count++;
    return true;
}

bool BufferPool::get_page(const PageID& page_id, char* page_data) {
    if (page_data == nullptr) {
        return false;
    }

    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    auto bucket = directory[bucket_index];

    Page* page = bucket->find_page(page_id);
    if (page && page->is_valid) {
        page->reference_bit = true;
        std::memcpy(page_data, page->data, PAGE_SIZE);
        return true;
    }

    return false;
}

bool BufferPool::contains_page(const PageID& page_id) const {
    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    return directory[bucket_index]->contains(page_id);
}


bool BufferPool::remove_page(const PageID& page_id) {
    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    auto bucket = directory[bucket_index];

    if (bucket->remove_page(page_id)) {
        remove_from_clock_ring(nullptr);
        current_page_count--;
        return true;
    }

    return false;
}

void BufferPool::enable_eviction_policy(bool enable) {
    eviction_enabled = enable;
}

bool BufferPool::pin_page(const PageID& page_id) {
    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    auto bucket = directory[bucket_index];
    Page* page = bucket->find_page(page_id);
    if (!page) return false;
    page->pin_count++;
    return true;
}

bool BufferPool::unpin_page(const PageID& page_id) {
    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    auto bucket = directory[bucket_index];
    Page* page = bucket->find_page(page_id);
    if (!page) return false;
    if (page->pin_count == 0) return false;
    page->pin_count--;
    return true;
}

bool BufferPool::mark_dirty(const PageID& page_id) {
    size_t hash_val = hash_page_id(page_id);
    size_t bucket_index = get_bucket_index(hash_val);
    auto bucket = directory[bucket_index];
    Page* page = bucket->find_page(page_id);
    if (!page) return false;
    page->dirty = true;
    return true;
}

void BufferPool::remove_from_clock_ring(Page* page_ptr) {
    for (size_t i = 0; i < clock_ring.size(); ++i) {
        if (clock_ring[i] == page_ptr) {
            clock_ring[i] = nullptr;
            // Do not adjust clock_hand here; leave for evict_one to compact lazily
            break;
        }
    }
}

bool BufferPool::evict_one() {
    if (clock_ring.empty()) return false;

    size_t scanned = 0;
    size_t ring_size = clock_ring.size();
    size_t max_scans = ring_size * 2; // allow one full pass to clear refs, second to evict

    while (scanned < max_scans) {
        if (clock_hand >= clock_ring.size()) {
            clock_hand = 0;
        }

        Page* candidate = clock_ring[clock_hand];
        if (candidate == nullptr || !candidate->is_valid) {
            // remove null slots by erasing; keep hand at same index
            clock_ring.erase(clock_ring.begin() + clock_hand);
            ring_size = clock_ring.size();
            continue;
        }

        if (candidate->pin_count > 0) {
            clock_hand = (clock_hand + 1) % clock_ring.size();
            scanned++;
            continue;
        }

        if (candidate->reference_bit) {
            candidate->reference_bit = false;
            clock_hand = (clock_hand + 1) % clock_ring.size();
            scanned++;
            continue;
        }

        // Evict this candidate
        if (candidate->dirty && write_back) {
            write_back(candidate->page_id, candidate->data);
            candidate->dirty = false;
        }

        // Remove from its bucket
        size_t hash_val = hash_page_id(candidate->page_id);
        size_t bucket_index = get_bucket_index(hash_val);
        auto bucket = directory[bucket_index];
        bucket->remove_page(candidate->page_id);

        // Remove from ring
        clock_ring.erase(clock_ring.begin() + clock_hand);
        current_page_count--;
        return true;
    }

    return false;
}

// general helpers
size_t BufferPool::get_directory_size() const {
    return directory.size();
}

size_t BufferPool::get_global_depth() const {
    return global_depth;
}

size_t BufferPool::get_page_count() const {
    return current_page_count;
}

size_t BufferPool::get_max_pages() const {
    return max_pages;
}

bool BufferPool::is_full() const {
    return current_page_count >= max_pages;
}
//

void BufferPool::clear() {
    directory.clear();
    size_t dir_size = 1 << initial_depth;
    directory.resize(dir_size);

    for (size_t i = 0; i < dir_size; i++) {
        directory[i] = std::make_shared<Bucket>(initial_depth, pages_per_bucket);
    }

    global_depth = initial_depth;
    current_page_count = 0;
    clock_ring.clear();
    clock_hand = 0;
}

void BufferPool::print_stats() const {
    std::cout << "\n== Extendible Hashing Buffer Pool Statistics ===" << std::endl;
    std::cout << "Global depth: " << global_depth << std::endl;
    std::cout << "Directory size: " << directory.size() << std::endl;
    std::cout << "Current pages: " << current_page_count << std::endl;
    std::cout << "Max pages: " << max_pages << std::endl;
    std::cout << "Pages per bucket: " << pages_per_bucket << std::endl;

    size_t unique_buckets = 0;
    std::set<Bucket*> seen;

    for (const auto& bucket : directory) {
        if (seen.find(bucket.get()) == seen.end()) {
            seen.insert(bucket.get());
            unique_buckets++;
        }
    }

    std::cout << "Unique buckets: " << unique_buckets << std::endl;
    std::cout << "Load factor: " << static_cast<double>(current_page_count) / (unique_buckets * pages_per_bucket) << std::endl;
}

#endif
