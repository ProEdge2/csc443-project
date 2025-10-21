#include "test_framework.h"
#include "../src/core/database.h"
#include "../src/buffer/buffer_pool.h"
#include <filesystem>
#include <iostream>
#include <chrono>

void test_database_with_buffer_pool_basic() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_buffer_integration_basic");
    Database<int, int> db("test_buffer_integration_basic", 3);

    ASSERT_TRUE(db.open());

    // Insert data to trigger SST creation
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300));
    ASSERT_TRUE(db.put(4, 400)); // This should trigger SST flush

    // Verify data can be retrieved (should use buffer pool)
    int value;
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(200, value);

    ASSERT_TRUE(db.get(3, value));
    ASSERT_EQUAL(300, value);

    ASSERT_TRUE(db.get(4, value));
    ASSERT_EQUAL(400, value);

    ASSERT_TRUE(db.close());
}

void test_database_with_buffer_pool_scan() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_buffer_integration_scan");
    Database<int, int> db("test_buffer_integration_scan", 3);

    ASSERT_TRUE(db.open());

    // Insert data to create multiple SSTs
    for (int i = 1; i <= 6; i++) {
        ASSERT_TRUE(db.put(i, i * 100));
    }

    // Test scan operations (should use buffer pool for page access)
    size_t result_size;
    std::pair<int, int>* results = db.scan(2, 5, result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(4, static_cast<int>(result_size));

    // Verify scan results
    std::vector<std::pair<int, int>> expected = {{2, 200}, {3, 300}, {4, 400}, {5, 500}};
    for (size_t i = 0; i < result_size; i++) {
        ASSERT_EQUAL(expected[i].first, results[i].first);
        ASSERT_EQUAL(expected[i].second, results[i].second);
    }

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_buffer_pool_caching() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_buffer_caching");
    Database<int, int> db("test_buffer_caching", 2);

    ASSERT_TRUE(db.open());

    // Insert data to create SST
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300)); // Triggers SST flush

    // Multiple retrievals should benefit from buffer pool caching
    int value;
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(db.get(1, value));
        ASSERT_EQUAL(100, value);

        ASSERT_TRUE(db.get(2, value));
        ASSERT_EQUAL(200, value);

        ASSERT_TRUE(db.get(3, value));
        ASSERT_EQUAL(300, value);
    }

    ASSERT_TRUE(db.close());
}

void test_database_buffer_pool_multiple_ssts() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_buffer_multiple_ssts");
    Database<int, int> db("test_buffer_multiple_ssts", 2);

    ASSERT_TRUE(db.open());

    // Create multiple SSTs
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300)); // First SST
    ASSERT_TRUE(db.put(4, 400));
    ASSERT_TRUE(db.put(5, 500)); // Second SST
    ASSERT_TRUE(db.put(6, 600));
    ASSERT_TRUE(db.put(7, 700)); // Third SST

    // Test retrieval from different SSTs (buffer pool should cache pages)
    int value;
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.get(4, value));
    ASSERT_EQUAL(400, value);

    ASSERT_TRUE(db.get(7, value));
    ASSERT_EQUAL(700, value);

    // Test scan across multiple SSTs
    size_t result_size;
    std::pair<int, int>* results = db.scan(2, 6, result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(5, static_cast<int>(result_size));

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_buffer_pool_persistence() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_buffer_persistence");

    // First session - create data
    {
        Database<int, int> db("test_buffer_persistence", 2);
        ASSERT_TRUE(db.open());

        ASSERT_TRUE(db.put(1, 100));
        ASSERT_TRUE(db.put(2, 200));
        ASSERT_TRUE(db.put(3, 300)); // Triggers SST flush
        ASSERT_TRUE(db.put(4, 400));
        ASSERT_TRUE(db.put(5, 500)); // Triggers second SST flush

        ASSERT_TRUE(db.close());
    }

    // Second session - load existing SSTs (should use buffer pool)
    {
        Database<int, int> db("test_buffer_persistence", 2);
        ASSERT_TRUE(db.open());

        int value;
        ASSERT_TRUE(db.get(1, value));
        ASSERT_EQUAL(100, value);

        ASSERT_TRUE(db.get(3, value));
        ASSERT_EQUAL(300, value);

        ASSERT_TRUE(db.get(5, value));
        ASSERT_EQUAL(500, value);

        // Test scan across persisted SSTs
        size_t result_size;
        std::pair<int, int>* results = db.scan(2, 4, result_size);

        ASSERT_TRUE(results != nullptr);
        ASSERT_EQUAL(3, static_cast<int>(result_size));

        delete[] results;
        ASSERT_TRUE(db.close());
    }
}

void test_database_buffer_pool_performance() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_buffer_performance");
    Database<int, int> db("test_buffer_performance", 10);

    ASSERT_TRUE(db.open());

    // Insert large amount of data to create multiple SSTs
    for (int i = 1; i <= 20; i++) {
        ASSERT_TRUE(db.put(i, i * 100));
    }

    // Measure time for multiple retrievals (should benefit from buffer pool)
    auto start = std::chrono::high_resolution_clock::now();

    int value;
    for (int round = 0; round < 10; round++) {
        for (int i = 1; i <= 20; i++) {
            ASSERT_TRUE(db.get(i, value));
            ASSERT_EQUAL(i * 100, value);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    std::cout << "Buffer pool performance test completed in " << duration.count() << "ms" << std::endl;

    ASSERT_TRUE(db.close());
}

int main() {
    std::cout << "Running Buffer Pool Integration Tests" << std::endl;
    std::cout << "====================================" << std::endl;

    RUN_TEST(test_database_with_buffer_pool_basic);
    RUN_TEST(test_database_with_buffer_pool_scan);
    RUN_TEST(test_database_buffer_pool_caching);
    RUN_TEST(test_database_buffer_pool_multiple_ssts);
    RUN_TEST(test_database_buffer_pool_persistence);
    RUN_TEST(test_database_buffer_pool_performance);

    TestFramework::print_results();
    return TestFramework::tests_run - TestFramework::tests_passed;
}
