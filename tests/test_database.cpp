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
    // Database<std::string, std::string> db("test_db_put_get", 10);

    // ASSERT_TRUE(db.open());

    // ASSERT_TRUE(db.put("key1", "value1"));
    // ASSERT_TRUE(db.put("key2", "value2"));

    // std::string value;
    // ASSERT_TRUE(db.get("key1", value));
    // ASSERT_EQUAL(std::string("value1"), value);

    // ASSERT_TRUE(db.get("key2", value));
    // ASSERT_EQUAL(std::string("value2"), value);

    // ASSERT_FALSE(db.get("nonexistent", value));

    // ASSERT_TRUE(db.close());
}

int main() {
    std::cout << "Running Database Tests" << std::endl;

    RUN_TEST(test_database_open_close);
    RUN_TEST(test_database_put_get);

    // TODO: ...
    // RUN_TEST(test_database_scan_basic);
    // RUN_TEST(test_database_scan_across_memtable_and_sst);
    // RUN_TEST(test_database_get_from_multiple_ssts);
    // RUN_TEST(test_database_youngest_to_oldest_search);
    // RUN_TEST(test_sst_binary_search);
    // RUN_TEST(test_sst_scan_range);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
