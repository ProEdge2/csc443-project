#include "test_framework.h"
#include "../src/core/database.h"
#include <string>

void test_memtable_flush_to_sst() {
    Database<std::string, int> db("test_flush_db", 3); // Small memtable size

    ASSERT_TRUE(db.open());

    // Fill memtable to capacity
    ASSERT_TRUE(db.put("key1", 1));
    ASSERT_TRUE(db.put("key2", 2));
    ASSERT_TRUE(db.put("key3", 3));

    // Check memtable is full
    ASSERT_EQUAL(3, static_cast<int>(db.get_memtable_size()));

    // Manually flush memtable to SST
    db.flush_memtable_to_sst();

    // Check memtable is now empty
    ASSERT_EQUAL(0, static_cast<int>(db.get_memtable_size()));

    // Check SST file was created
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_sst_creation_with_sorted_data() {
    Database<std::string, int> db("test_sst_creation", 5);

    ASSERT_TRUE(db.open());

    // Insert data in non-sorted order
    ASSERT_TRUE(db.put("cherry", 3));
    ASSERT_TRUE(db.put("apple", 1));
    ASSERT_TRUE(db.put("banana", 2));
    ASSERT_TRUE(db.put("elderberry", 5));
    ASSERT_TRUE(db.put("date", 4));

    // Flush to SST
    db.flush_memtable_to_sst();

    // Verify memtable is empty
    ASSERT_EQUAL(0, static_cast<int>(db.get_memtable_size()));

    // Verify SST was created
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_multiple_sst_files() {
    Database<std::string, int> db("test_multiple_sst", 2); // Small memtable

    ASSERT_TRUE(db.open());

    // First flush
    ASSERT_TRUE(db.put("key1", 1));
    ASSERT_TRUE(db.put("key2", 2));
    db.flush_memtable_to_sst();
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    // Second flush
    ASSERT_TRUE(db.put("key3", 3));
    ASSERT_TRUE(db.put("key4", 4));
    db.flush_memtable_to_sst();
    ASSERT_EQUAL(2, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_flush_empty_memtable() {
    Database<std::string, int> db("test_flush_empty", 5);

    ASSERT_TRUE(db.open());

    // Try to flush empty memtable
    db.flush_memtable_to_sst();

    // Should not create any SST files
    ASSERT_EQUAL(0, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_flush_with_string_values() {
    Database<std::string, std::string> db("test_flush_strings", 3);

    ASSERT_TRUE(db.open());

    // Insert string values
    ASSERT_TRUE(db.put("name", "Alice"));
    ASSERT_TRUE(db.put("age", "25"));
    ASSERT_TRUE(db.put("city", "New York"));

    // Flush to SST
    db.flush_memtable_to_sst();

    // Verify flush worked
    ASSERT_EQUAL(0, static_cast<int>(db.get_memtable_size()));
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_flush_with_integer_keys() {
    Database<int, std::string> db("test_flush_ints", 4);

    ASSERT_TRUE(db.open());

    // Insert with integer keys
    ASSERT_TRUE(db.put(1, "one"));
    ASSERT_TRUE(db.put(2, "two"));
    ASSERT_TRUE(db.put(3, "three"));
    ASSERT_TRUE(db.put(4, "four"));

    // Flush to SST
    db.flush_memtable_to_sst();

    // Verify flush worked
    ASSERT_EQUAL(0, static_cast<int>(db.get_memtable_size()));
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_database_close_flushes_memtable() {
    Database<std::string, int> db("test_close_flush", 5);

    ASSERT_TRUE(db.open());

    // Insert some data
    ASSERT_TRUE(db.put("key1", 1));
    ASSERT_TRUE(db.put("key2", 2));
    ASSERT_TRUE(db.put("key3", 3));

    // Close database (should flush memtable)
    ASSERT_TRUE(db.close());

    // Reopen and check that SST was created
    ASSERT_TRUE(db.open());
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));
    ASSERT_TRUE(db.close());
}

int main() {
    std::cout << "Running SST Flush Tests" << std::endl;

    RUN_TEST(test_memtable_flush_to_sst);
    RUN_TEST(test_sst_creation_with_sorted_data);
    RUN_TEST(test_multiple_sst_files);
    RUN_TEST(test_flush_empty_memtable);
    RUN_TEST(test_flush_with_string_values);
    RUN_TEST(test_flush_with_integer_keys);
    RUN_TEST(test_database_close_flushes_memtable);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
