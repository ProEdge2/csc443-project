#include "test_framework.h"
#include "../src/buffer/buffer_pool.h"
#include "../src/core/database.h"
#include <iostream>
#include <vector>
#include <chrono>

void test_short_scan_behavior() {
    std::cout << "Testing short scan behavior (should keep pages in buffer pool)..." << std::endl;

    // Create a database with small buffer pool and low flooding threshold
    Database<int, int> db("test_short_scan", 1000);
    db.open();

    // Insert some data to create SST files
    for (int i = 0; i < 50; i++) {
        db.put(i, i * 10);
    }

    // Force flush to create SST
    db.flush_memtable_to_sst();

    // Perform a short scan (should be under threshold)
    size_t result_size;
    auto results = db.scan(0, 9, result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(10, result_size);

    // Verify scan worked correctly
    for (size_t i = 0; i < result_size; i++) {
        ASSERT_EQUAL(static_cast<int>(i), results[i].first);
        ASSERT_EQUAL(static_cast<int>(i * 10), results[i].second);
    }

    delete[] results;
    db.close();

    std::cout << "Short scan test passed!" << std::endl;
}

void test_long_scan_behavior() {
    std::cout << "Testing long scan behavior (should mark pages for eviction)..." << std::endl;

    // Create a database with small buffer pool and low flooding threshold
    Database<int, int> db("test_long_scan", 1000);
    db.open();

    // Insert more data to create larger SST files
    for (int i = 0; i < 200; i++) {
        db.put(i, i * 10);
    }

    // Force flush to create SST
    db.flush_memtable_to_sst();

    // Perform a long scan (should be over threshold)
    size_t result_size;
    auto results = db.scan(0, 199, result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(200, result_size);

    // Verify scan worked correctly
    for (size_t i = 0; i < result_size; i++) {
        ASSERT_EQUAL(static_cast<int>(i), results[i].first);
        ASSERT_EQUAL(static_cast<int>(i * 10), results[i].second);
    }

    delete[] results;
    db.close();

    std::cout << "Long scan test passed!" << std::endl;
}

void test_buffer_pool_scan_tracking() {
    std::cout << "Testing buffer pool scan tracking..." << std::endl;

    // Create buffer pool with low threshold
    BufferPool pool(2, 10, 4, 20, true, nullptr, 5); // threshold = 5 pages

    // Test scan tracking
    std::string scan_id = pool.begin_scan();
    ASSERT_TRUE(!scan_id.empty());

    // Simulate accessing pages
    for (int i = 0; i < 3; i++) {
        PageID pid("test.sst", i);
        pool.access_page_for_scan(scan_id, pid);
    }

    // End scan (should be under threshold, so no priority changes)
    pool.end_scan(scan_id);

    // Test long scan
    scan_id = pool.begin_scan();
    for (int i = 0; i < 10; i++) {
        PageID pid("test.sst", i);
        pool.access_page_for_scan(scan_id, pid);
    }

    // End scan (should be over threshold, so pages get marked)
    pool.end_scan(scan_id);

    std::cout << "Buffer pool scan tracking test passed!" << std::endl;
}

int main() {
    std::cout << "=== Sequential Flooding Protection Tests ===" << std::endl;

    try {
        test_short_scan_behavior();
        test_long_scan_behavior();
        test_buffer_pool_scan_tracking();

        TestFramework::print_results();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}
