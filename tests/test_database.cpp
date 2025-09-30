#include "test_framework.h"
#include "../src/core/database.h"
#include <string>

void test_database_open_close() {
    Database<std::string, std::string> db("test_db_open_close", 5);

    ASSERT_FALSE(db.is_database_open());
    ASSERT_TRUE(db.open());
    ASSERT_TRUE(db.is_database_open());
    ASSERT_TRUE(db.close());
    ASSERT_FALSE(db.is_database_open());
}

void test_database_put_get() {
    Database<std::string, std::string> db("test_db_put_get", 10);

    ASSERT_TRUE(db.open());

    ASSERT_TRUE(db.put("key1", "value1"));
    ASSERT_TRUE(db.put("key2", "value2"));

    std::string value;
    ASSERT_TRUE(db.get("key1", value));
    ASSERT_EQUAL(std::string("value1"), value);

    ASSERT_TRUE(db.get("key2", value));
    ASSERT_EQUAL(std::string("value2"), value);

    ASSERT_FALSE(db.get("nonexistent", value));

    ASSERT_TRUE(db.close());
}

void test_database_scan_basic() {
    Database<std::string, int> db("test_scan_basic", 10);

    ASSERT_TRUE(db.open());

    // Insert test data
    ASSERT_TRUE(db.put("apple", 1));
    ASSERT_TRUE(db.put("banana", 2));
    ASSERT_TRUE(db.put("cherry", 3));
    ASSERT_TRUE(db.put("date", 4));
    ASSERT_TRUE(db.put("elderberry", 5));

    // Test basic scan
    size_t result_size;
    std::pair<std::string, int>* results = db.scan("banana", "date", result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(3, static_cast<int>(result_size));

    // Check results are in order
    ASSERT_EQUAL(std::string("banana"), results[0].first);
    ASSERT_EQUAL(2, results[0].second);
    ASSERT_EQUAL(std::string("cherry"), results[1].first);
    ASSERT_EQUAL(3, results[1].second);
    ASSERT_EQUAL(std::string("date"), results[2].first);
    ASSERT_EQUAL(4, results[2].second);

    // Clean up
    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_scan_empty_result() {
    Database<std::string, int> db("test_scan_empty", 10);

    ASSERT_TRUE(db.open());

    // Insert some data
    ASSERT_TRUE(db.put("apple", 1));
    ASSERT_TRUE(db.put("banana", 2));

    // Test scan with no matches
    size_t result_size;
    std::pair<std::string, int>* results = db.scan("zebra", "zoo", result_size);

    ASSERT_TRUE(results == nullptr);
    ASSERT_EQUAL(0, static_cast<int>(result_size));

    ASSERT_TRUE(db.close());
}

void test_database_scan_single_key() {
    Database<std::string, int> db("test_scan_single", 10);

    ASSERT_TRUE(db.open());

    ASSERT_TRUE(db.put("apple", 1));
    ASSERT_TRUE(db.put("banana", 2));
    ASSERT_TRUE(db.put("cherry", 3));

    // Test scan with single key match
    size_t result_size;
    std::pair<std::string, int>* results = db.scan("banana", "banana", result_size);

    ASSERT_TRUE(results != nullptr);
    ASSERT_EQUAL(1, static_cast<int>(result_size));
    ASSERT_EQUAL(std::string("banana"), results[0].first);
    ASSERT_EQUAL(2, results[0].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_database_scan_closed_database() {
    Database<std::string, int> db("test_scan_closed", 10);

    // Test scan on closed database
    size_t result_size;
    std::pair<std::string, int>* results = db.scan("apple", "banana", result_size);

    ASSERT_TRUE(results == nullptr);
    ASSERT_EQUAL(0, static_cast<int>(result_size));
}

int main() {
    std::cout << "Running Database Tests" << std::endl;

    RUN_TEST(test_database_open_close);
    RUN_TEST(test_database_put_get);
    RUN_TEST(test_database_scan_basic);
    RUN_TEST(test_database_scan_empty_result);
    RUN_TEST(test_database_scan_single_key);
    RUN_TEST(test_database_scan_closed_database);

    // TODO: SST-related tests (when SST scanning is implemented)
    // RUN_TEST(test_database_scan_across_memtable_and_sst);
    // RUN_TEST(test_database_get_from_multiple_ssts);
    // RUN_TEST(test_database_youngest_to_oldest_search);
    // RUN_TEST(test_sst_binary_search);
    // RUN_TEST(test_sst_scan_range);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
