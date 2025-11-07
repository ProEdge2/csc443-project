#include "test_framework.h"
#include "../src/core/database.h"
#include <string>

void test_memtable_flush_to_sst() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_flush_db");
    Database<int, int> db("test_flush_db", 3); // Small memtable size

    ASSERT_TRUE(db.open());

    // Fill memtable to capacity
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300));

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
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_sst_creation");
    Database<int, int> db("test_sst_creation", 5);

    ASSERT_TRUE(db.open());

    // Insert data in non-sorted order
    ASSERT_TRUE(db.put(5, 3));
    ASSERT_TRUE(db.put(4, 1));
    ASSERT_TRUE(db.put(3, 2));
    ASSERT_TRUE(db.put(2, 5));
    ASSERT_TRUE(db.put(1, 4));

    // Flush to SST
    db.flush_memtable_to_sst();

    // Verify memtable is empty
    ASSERT_EQUAL(0, static_cast<int>(db.get_memtable_size()));

    // Verify SST was created
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_multiple_sst_files() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_multiple_sst");
    Database<int, int> db("test_multiple_sst", 2); // Small memtable

    ASSERT_TRUE(db.open());

    // First flush
    ASSERT_TRUE(db.put(1, 1));
    ASSERT_TRUE(db.put(2, 2));
    db.flush_memtable_to_sst();
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    // Second flush
    ASSERT_TRUE(db.put(3, 3));
    ASSERT_TRUE(db.put(4, 4));
    db.flush_memtable_to_sst();
    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_flush_empty_memtable() {
    Database<int, int> db("test_flush_empty", 5);

    ASSERT_TRUE(db.open());

    // Try to flush empty memtable
    db.flush_memtable_to_sst();

    // Should not create any SST files
    ASSERT_EQUAL(0, static_cast<int>(db.get_sst_count()));

    ASSERT_TRUE(db.close());
}

void test_database_close_flushes_memtable() {
    // Clear SSTs from previous run
    std::filesystem::remove_all("data/test_close_flush");
    Database<int, int> db("test_close_flush", 5);

    ASSERT_TRUE(db.open());

    // Insert some data
    ASSERT_TRUE(db.put(1, 100));
    ASSERT_TRUE(db.put(2, 200));
    ASSERT_TRUE(db.put(3, 300));

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
    RUN_TEST(test_database_close_flushes_memtable);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
