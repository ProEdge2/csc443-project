#include "test_framework.h"
#include "../src/core/database.h"
#include <string>
#include <filesystem>

void test_database_open_close() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_db_open_close");
    Database<int, int> db("test_db_open_close", 5);

    ASSERT_FALSE(db.is_database_open());
    ASSERT_TRUE(db.open());
    ASSERT_TRUE(db.is_database_open());
    ASSERT_TRUE(db.close());
    ASSERT_FALSE(db.is_database_open());
}

void test_database_put_get() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_db_put_get");
    Database<int, int> db("test_db_put_get", 10);

    ASSERT_TRUE(db.open());

    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));

    int value;
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(200, value);

    ASSERT_FALSE(db.get(3, value));

    ASSERT_TRUE(db.close());
}

void test_database_scan_basic() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_basic");
    Database<int, int> db("test_scan_basic", 10);

    ASSERT_TRUE(db.open());

    // Insert test data
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300));
    ASSERT_TRUE(db.put(4, 400));
    ASSERT_TRUE(db.put(5, 500));

    // Test basic scan
    size_t result_size;
    std::pair<int, int>* results = db.scan(1, 3, result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(3, static_cast<int>(result_size));

    // Check results are in order
    ASSERT_EQUAL(1, results[0].first);
    ASSERT_EQUAL(100, results[0].second);
    ASSERT_EQUAL(2, results[1].first);
    ASSERT_EQUAL(200, results[1].second);
    ASSERT_EQUAL(3, results[2].first);
    ASSERT_EQUAL(300, results[2].second);

    // Clean up
    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_scan_empty_result() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_empty");
    Database<int, int> db("test_scan_empty", 10);

    ASSERT_TRUE(db.open());

    // Insert some data
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));

    // Test scan with no matches
    size_t result_size;
    std::pair<int, int>* results = db.scan(3, 5, result_size);

    ASSERT_TRUE(results == nullptr);
    ASSERT_EQUAL(0, static_cast<int>(result_size));

    ASSERT_TRUE(db.close());
}

void test_database_scan_single_key() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_single");
    Database<int, int> db("test_scan_single", 10);

    ASSERT_TRUE(db.open());

    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300));

    // Test scan with single key match
    size_t result_size;
    std::pair<int, int>* results = db.scan(1, 1, result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(1, static_cast<int>(result_size));
    ASSERT_EQUAL(1, results[0].first);
    ASSERT_EQUAL(100, results[0].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_scan_closed_database() {
    Database<int, int> db("test_scan_closed", 10);

    // Test scan on closed database
    size_t result_size;
    std::pair<int, int>* results = db.scan(1, 2, result_size);

    ASSERT_TRUE(results == nullptr);
    ASSERT_EQUAL(0, static_cast<int>(result_size));
}

void test_database_get_from_multiple_ssts_b_tree() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_database_get_from_multiple_ssts_b_tree");
    Database<int, int> db("test_database_get_from_multiple_ssts_b_tree", 2);
    ASSERT_TRUE(db.open());

    // Create multiple SSTs
    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    db.put(3, 300);
    db.put(4, 400);
    db.put(5, 500);
    db.flush_memtable_to_sst();

    int value;
    ASSERT_TRUE(db.get(1, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.get(3, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(300, value);

    ASSERT_TRUE(db.get(5, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(500, value);

    ASSERT_FALSE(db.get(10, value, SearchMode::B_TREE_SEARCH));

    ASSERT_TRUE(db.close());
}

void test_database_get_from_multiple_ssts_binary_search() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_database_get_from_multiple_ssts_binary_search");
    Database<int, int> db("test_database_get_from_multiple_ssts_binary_search", 2);
    ASSERT_TRUE(db.open());

    // Create multiple SSTs
    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    db.put(3, 300);
    db.put(4, 400);
    db.put(5, 500);
    db.flush_memtable_to_sst();

    int value;
    ASSERT_TRUE(db.get(1, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.get(3, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(300, value);

    ASSERT_TRUE(db.get(5, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(500, value);

    ASSERT_FALSE(db.get(10, value, SearchMode::BINARY_SEARCH));

    ASSERT_TRUE(db.close());
}

void test_database_scan_across_memtable_and_sst_b_tree() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_database_scan_across_memtable_and_sst_b_tree");
    Database<int, int> db("test_database_scan_across_memtable_and_sst_b_tree", 3);
    ASSERT_TRUE(db.open());

    // Flush to SST
    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    // Add to memtable
    db.put(4, 400);
    db.put(5, 500);

    size_t result_size = 0;
    auto results = db.scan(2, 5, result_size, SearchMode::B_TREE_SEARCH);

    ASSERT_EQUAL(4, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(200, results[0].second);
    ASSERT_EQUAL(3, results[1].first);
    ASSERT_EQUAL(300, results[1].second);
    ASSERT_EQUAL(4, results[2].first);
    ASSERT_EQUAL(400, results[2].second);
    ASSERT_EQUAL(5, results[3].first);
    ASSERT_EQUAL(500, results[3].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_scan_across_memtable_and_sst_binary_search() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_database_scan_across_memtable_and_sst_binary_search");
    Database<int, int> db("test_database_scan_across_memtable_and_sst_binary_search", 3);
    ASSERT_TRUE(db.open());

    // Flush to SST
    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    // Add to memtable
    db.put(4, 400);
    db.put(5, 500);

    size_t result_size = 0;
    auto results = db.scan(2, 5, result_size, SearchMode::BINARY_SEARCH);

    ASSERT_EQUAL(4, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(200, results[0].second);
    ASSERT_EQUAL(3, results[1].first);
    ASSERT_EQUAL(300, results[1].second);
    ASSERT_EQUAL(4, results[2].first);
    ASSERT_EQUAL(400, results[2].second);
    ASSERT_EQUAL(5, results[3].first);
    ASSERT_EQUAL(500, results[3].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_get_youngest_to_oldest_search() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_get_youngest_to_oldest_search");
    Database<int, int> db("test_get_youngest_to_oldest_search", 2);
    ASSERT_TRUE(db.open());

    // Insert and flush to SST (older)
    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    // Overwrite key=2 in memtable (younger)
    db.put(2, 999);

    int value;
    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(999, value); // youngest wins

    ASSERT_TRUE(db.close());
}

void test_database_get_youngest_to_oldest_search_across_multiple_ssts() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_get_youngest_to_oldest_search_across_multiple_ssts");
    Database<int, int> db("test_get_youngest_to_oldest_search_across_multiple_ssts", 2);
    ASSERT_TRUE(db.open());

    // Insert and flush to SST (older)
    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    // Insert 2 and flush to SST (younger)
    db.put(2, 999);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    int value;
    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(999, value); // youngest wins

    ASSERT_TRUE(db.close());
}

void test_database_scan_youngest_to_oldest_search() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_youngest_to_oldest_search");
    Database<int, int> db("test_scan_youngest_to_oldest_search", 3);
    ASSERT_TRUE(db.open());

    // Flush to SST
    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    // memtable has key=3 (younger)
    db.put(3, 999);
    db.put(4, 400);
    db.put(5, 500);

    size_t result_size = 0;
    auto results = db.scan(2, 5, result_size);

    ASSERT_EQUAL(4, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(200, results[0].second);
    ASSERT_EQUAL(3, results[1].first);
    ASSERT_EQUAL(999, results[1].second); // expect younger wins
    ASSERT_EQUAL(4, results[2].first);
    ASSERT_EQUAL(400, results[2].second);
    ASSERT_EQUAL(5, results[3].first);
    ASSERT_EQUAL(500, results[3].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_scan_youngest_to_oldest_search_across_multiple_ssts() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_youngest_to_oldest_search_across_multiple_ssts");
    Database<int, int> db("test_scan_youngest_to_oldest_search_across_multiple_ssts", 3);
    ASSERT_TRUE(db.open());

    // Flush to SST1 (older)
    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    // Flush to SST2 (younger)
    db.put(2, 222);
    db.put(3, 333);
    db.put(4, 400);
    db.flush_memtable_to_sst();

    size_t result_size = 0;
    auto results = db.scan(2, 5, result_size);

    ASSERT_EQUAL(3, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(222, results[0].second);
    ASSERT_EQUAL(3, results[1].first);
    ASSERT_EQUAL(333, results[1].second);
    ASSERT_EQUAL(4, results[2].first);
    ASSERT_EQUAL(400, results[2].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_scan_starts_before_smallest_key() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_starts_before_smallest_key");
    Database<int, int> db("test_scan_starts_before_smallest_key", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    size_t result_size = 0;
    auto results = db.scan(-5, 2, result_size); // includes key 1,2

    ASSERT_EQUAL(2, static_cast<int>(result_size));
    ASSERT_EQUAL(1, results[0].first);
    ASSERT_EQUAL(100, results[0].second);
    ASSERT_EQUAL(2, results[1].first);
    ASSERT_EQUAL(200, results[1].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_scan_ends_after_largest_key() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_ends_after_largest_key");
    Database<int, int> db("test_scan_ends_after_largest_key", 3);
    ASSERT_TRUE(db.open());

    db.put(8, 800);
    db.put(9, 900);
    db.flush_memtable_to_sst();

    size_t result_size = 0;
    auto results = db.scan(8, 20, result_size); // includes key 8,9

    ASSERT_EQUAL(2, static_cast<int>(result_size));
    ASSERT_EQUAL(8, results[0].first);
    ASSERT_EQUAL(800, results[0].second);
    ASSERT_EQUAL(9, results[1].first);
    ASSERT_EQUAL(900, results[1].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_scan_with_no_results() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_with_no_results");
    Database<int, int> db("test_scan_with_no_results", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    db.put(3, 300);

    size_t result_size = 0;
    auto results = db.scan(10, 20, result_size);

    ASSERT_EQUAL(0, static_cast<int>(result_size));
    ASSERT_TRUE(results == nullptr);

    ASSERT_TRUE(db.close());
}

void test_scan_exactly_one_key() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_exactly_one_key");
    Database<int, int> db("test_scan_exactly_one_key", 3);
    ASSERT_TRUE(db.open());

    db.put(5, 500);
    db.put(6, 600);
    db.flush_memtable_to_sst();

    size_t result_size = 0;
    auto results = db.scan(5, 5, result_size);

    ASSERT_EQUAL(1, static_cast<int>(result_size));
    ASSERT_EQUAL(5, results[0].first);
    ASSERT_EQUAL(500, results[0].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_scan_spanning_multiple_ssts_b_tree() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_spanning_multiple_ssts");
    Database<int, int> db("test_scan_spanning_multiple_ssts", 2); // small memtable size
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    db.put(3, 300);
    db.put(4, 400);
    db.flush_memtable_to_sst();

    db.put(5, 500);
    db.put(6, 600);
    db.put(7, 700);
    db.flush_memtable_to_sst();

    size_t result_size = 0;
    auto results = db.scan(2, 7, result_size, SearchMode::B_TREE_SEARCH);

    ASSERT_EQUAL(6, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(200, results[0].second);
    ASSERT_EQUAL(7, results[5].first);
    ASSERT_EQUAL(700, results[5].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_scan_spanning_multiple_ssts_binary_search() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_scan_spanning_multiple_ssts_binary_search");
    Database<int, int> db("test_scan_spanning_multiple_ssts_binary_search", 2); // small memtable size
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst();

    db.put(3, 300);
    db.put(4, 400);
    db.flush_memtable_to_sst();

    db.put(5, 500);
    db.put(6, 600);
    db.put(7, 700);
    db.flush_memtable_to_sst();

    size_t result_size = 0;
    auto results = db.scan(2, 7, result_size, SearchMode::BINARY_SEARCH);

    ASSERT_EQUAL(6, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(200, results[0].second);
    ASSERT_EQUAL(7, results[5].first);
    ASSERT_EQUAL(700, results[5].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_sst_preserved_across_db() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_sst_preserved_across_db");
    Database<int, int> db("test_sst_preserved_across_db", 3);
    ASSERT_TRUE(db.open());

    // Generate SSTs
    db.put(1, 100);
    db.put(2, 200);
    db.flush_memtable_to_sst(); // older SST

    db.put(2, 999);
    db.put(3, 300);

    // Close DB (should flush memtable to sst)
    ASSERT_TRUE(db.close());

    // Open DB (open loads existing SSTs)
    Database<int, int> db2("test_sst_preserved_across_db", 3);
    ASSERT_TRUE(db2.open());

    // Check previous SSTs loaded
    // after compaction, 2 SSTs at level 0 become 1 SST at level 1
    ASSERT_EQUAL(1, static_cast<int>(db2.get_sst_count()));

    // Get
    int value;
    ASSERT_TRUE(db2.get(1, value));
    ASSERT_EQUAL(100, value);

    // Youngest wins for key 2
    ASSERT_TRUE(db2.get(2, value));
    ASSERT_EQUAL(999, value);

    // Scan
    size_t result_size = 0;
    auto results = db2.scan(1, 3, result_size);

    ASSERT_EQUAL(3, static_cast<int>(result_size));
    ASSERT_EQUAL(1, results[0].first);
    ASSERT_EQUAL(100, results[0].second);
    ASSERT_EQUAL(2, results[1].first);
    ASSERT_EQUAL(999, results[1].second);  // youngest wins
    ASSERT_EQUAL(3, results[2].first);
    ASSERT_EQUAL(300, results[2].second);

    ASSERT_TRUE(db2.close());
}

void test_sst_with_buffer_pool_caching() {
    std::filesystem::remove_all("data/test_buffer_pool_caching");

    Database<int, int> db("test_buffer_pool_caching", 100);
    ASSERT_TRUE(db.open());

    // Create an SST file
    for (int i = 0; i < 50; i++) {
        db.put(i, i * 10);
    }
    db.flush_memtable_to_sst();

    // should read from disk
    int value;
    ASSERT_TRUE(db.get(25, value));
    ASSERT_EQUAL(250, value);

    // should read from buffer (same page)
    ASSERT_TRUE(db.get(40, value));
    ASSERT_EQUAL(400, value);

    ASSERT_TRUE(db.close());
}

int main() {
    std::cout << "Running Database Tests" << std::endl;

    RUN_TEST(test_database_open_close);
    RUN_TEST(test_database_put_get);
    RUN_TEST(test_database_scan_basic);
    RUN_TEST(test_database_scan_empty_result);
    RUN_TEST(test_database_scan_single_key);
    RUN_TEST(test_database_scan_closed_database);

    // SST related tests
    RUN_TEST(test_database_get_from_multiple_ssts_b_tree);
    RUN_TEST(test_database_get_from_multiple_ssts_binary_search);
    RUN_TEST(test_database_scan_across_memtable_and_sst_b_tree);
    RUN_TEST(test_database_scan_across_memtable_and_sst_binary_search);

    RUN_TEST(test_database_get_youngest_to_oldest_search);
    RUN_TEST(test_database_get_youngest_to_oldest_search_across_multiple_ssts);
    RUN_TEST(test_database_scan_youngest_to_oldest_search);
    RUN_TEST(test_database_scan_youngest_to_oldest_search_across_multiple_ssts);

    RUN_TEST(test_scan_starts_before_smallest_key);
    RUN_TEST(test_scan_ends_after_largest_key);
    RUN_TEST(test_scan_with_no_results);
    RUN_TEST(test_scan_exactly_one_key);
    RUN_TEST(test_scan_spanning_multiple_ssts_b_tree);
    RUN_TEST(test_scan_spanning_multiple_ssts_binary_search);

    RUN_TEST(test_sst_preserved_across_db);

    RUN_TEST(test_sst_with_buffer_pool_caching);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
