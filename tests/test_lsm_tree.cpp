#include "test_framework.h"
#include "../src/core/database.h"
#include <string>
#include <filesystem>

void test_delete_basic() {
    std::filesystem::remove_all("data/test_delete_basic");
    Database<int, int> db("test_delete_basic", 10);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);

    int value;
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.remove(1));

    ASSERT_FALSE(db.get(1, value));
    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(200, value);

    ASSERT_TRUE(db.close());
}

void test_delete_and_reinsert() {
    std::filesystem::remove_all("data/test_delete_reinsert");
    Database<int, int> db("test_delete_reinsert", 10);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    ASSERT_TRUE(db.remove(1));

    int value;
    ASSERT_FALSE(db.get(1, value));

    db.put(1, 999);
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(999, value);

    ASSERT_TRUE(db.close());
}

void test_delete_from_sst() {
    std::filesystem::remove_all("data/test_delete_from_sst");
    Database<int, int> db("test_delete_from_sst", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    int value;
    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(200, value);

    db.remove(2);

    ASSERT_FALSE(db.get(2, value));
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);
    ASSERT_TRUE(db.get(3, value));
    ASSERT_EQUAL(300, value);


    ASSERT_TRUE(db.close());
}

void test_scan_with_deletes() {
    std::filesystem::remove_all("data/test_scan_deletes");
    Database<int, int> db("test_scan_deletes", 10);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.put(4, 400);
    db.put(5, 500);

    db.remove(3);

    size_t result_size;
    auto results = db.scan(1, 5, result_size);

    ASSERT_EQUAL(4, static_cast<int>(result_size));
    ASSERT_EQUAL(1, results[0].first);
    ASSERT_EQUAL(100, results[0].second);
    ASSERT_EQUAL(2, results[1].first);
    ASSERT_EQUAL(200, results[1].second);
    ASSERT_EQUAL(4, results[2].first);
    ASSERT_EQUAL(400, results[2].second);
    ASSERT_EQUAL(5, results[3].first);
    ASSERT_EQUAL(500, results[3].second);

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_compaction_basic() {
    std::filesystem::remove_all("data/test_compaction_basic");
    Database<int, int> db("test_compaction_basic", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    db.put(4, 400);
    db.put(5, 500);
    db.put(6, 600);
    db.flush_memtable_to_sst();

    ASSERT_EQUAL(1, static_cast<int>(db.get_sst_count()));

    int value;
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);
    ASSERT_TRUE(db.get(6, value));
    ASSERT_EQUAL(600, value);

    ASSERT_TRUE(db.close());
}

void test_compaction_preserves_youngest() {
    std::filesystem::remove_all("data/test_compaction_youngest");
    Database<int, int> db("test_compaction_youngest", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    db.put(2, 999);
    db.put(4, 400);
    db.put(5, 500);

    db.flush_memtable_to_sst();

    int value;
    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(999, value);

    ASSERT_TRUE(db.close());
}

void test_compaction_multiple_levels() {
    std::filesystem::remove_all("data/test_compaction_levels");
    Database<int, int> db("test_compaction_levels", 2);
    ASSERT_TRUE(db.open());

    for (int i = 0; i < 10; i++) {
        db.put(i * 10, i * 100);
        db.put(i * 10 + 1, i * 100 + 10);
        db.flush_memtable_to_sst();
    }

    for (int i = 0; i < 10; i++) {
        int value;
        ASSERT_TRUE(db.get(i * 10, value));
        ASSERT_EQUAL(i * 100, value);
    }

    ASSERT_TRUE(db.close());
}

void test_delete_persists_after_compaction() {
    std::filesystem::remove_all("data/test_delete_persist");
    Database<int, int> db("test_delete_persist", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    db.remove(2);
    db.put(4, 400);
    db.put(5, 500);

    db.flush_memtable_to_sst();

    int value;
    ASSERT_FALSE(db.get(2, value));

    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);

    ASSERT_TRUE(db.get(4, value));
    ASSERT_EQUAL(400, value);

    ASSERT_TRUE(db.close());
}

void test_scan_after_compaction() {
    std::filesystem::remove_all("data/test_scan_compaction");
    Database<int, int> db("test_scan_compaction", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    db.put(4, 400);
    db.put(5, 500);
    db.put(6, 600);
    db.flush_memtable_to_sst();

    size_t result_size;
    auto results = db.scan(1, 6, result_size);

    ASSERT_EQUAL(6, static_cast<int>(result_size));
    for (int i = 0; i < 6; i++) {
        ASSERT_EQUAL(i + 1, results[i].first);
        ASSERT_EQUAL((i + 1) * 100, results[i].second);
    }

    delete[] results;
    ASSERT_TRUE(db.close());
}

void test_compaction_with_overlapping_keys() {
    std::filesystem::remove_all("data/test_compaction_overlap");
    Database<int, int> db("test_compaction_overlap", 5);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.put(4, 400);
    db.put(5, 500);
    db.flush_memtable_to_sst();

    db.put(3, 333);
    db.put(4, 444);
    db.put(6, 600);
    db.put(7, 700);
    db.put(8, 800);
    db.flush_memtable_to_sst();

    int value;
    ASSERT_TRUE(db.get(1, value));
    ASSERT_EQUAL(100, value);
    ASSERT_TRUE(db.get(3, value));
    ASSERT_EQUAL(333, value);
    ASSERT_TRUE(db.get(4, value));
    ASSERT_EQUAL(444, value);
    ASSERT_TRUE(db.get(8, value));
    ASSERT_EQUAL(800, value);

    ASSERT_TRUE(db.close());
}

void test_delete_and_compact() {
    std::filesystem::remove_all("data/test_delete_compact");
    Database<int, int> db("test_delete_compact", 3);
    ASSERT_TRUE(db.open());

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.flush_memtable_to_sst();

    db.remove(1);
    db.remove(3);
    db.put(4, 400);
    db.flush_memtable_to_sst();

    int value;
    ASSERT_FALSE(db.get(1, value));
    ASSERT_TRUE(db.get(2, value));
    ASSERT_EQUAL(200, value);
    ASSERT_FALSE(db.get(3, value));
    ASSERT_TRUE(db.get(4, value));
    ASSERT_EQUAL(400, value);

    size_t result_size;
    auto results = db.scan(1, 4, result_size);
    ASSERT_EQUAL(2, static_cast<int>(result_size));
    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(200, results[0].second);
    ASSERT_EQUAL(4, results[1].first);
    ASSERT_EQUAL(400, results[1].second);

    delete[] results;

    ASSERT_TRUE(db.close());
}

void test_reopen_after_compaction() {
    std::filesystem::remove_all("data/test_reopen_compact");

    {
        Database<int, int> db("test_reopen_compact", 3);
        ASSERT_TRUE(db.open());

        db.put(1, 100);
        db.put(2, 200);
        db.put(3, 300);
        db.flush_memtable_to_sst();

        db.put(4, 400);
        db.put(5, 500);
        db.put(6, 600);
        db.flush_memtable_to_sst();

        ASSERT_TRUE(db.close());
     }

    {
        Database<int, int> db("test_reopen_compact", 3);
        ASSERT_TRUE(db.open());

        int value;
        for (int i = 1; i <= 6; i++) {
            ASSERT_TRUE(db.get(i, value));
            ASSERT_EQUAL(i * 100, value);
        }

        ASSERT_TRUE(db.close());
    }
}

int main() {
    std::cout << "\n=== Running LSM-Tree Tests ===" << std::endl;

    std::cout << "\n--- Delete/Tombstone Tests ---" << std::endl;
    RUN_TEST(test_delete_basic);
    RUN_TEST(test_delete_and_reinsert);
    RUN_TEST(test_delete_from_sst);
    RUN_TEST(test_scan_with_deletes);

    std::cout << "\n-- Compaction Tests --" << std::endl;
    RUN_TEST(test_compaction_basic);
    RUN_TEST(test_compaction_preserves_youngest);
    RUN_TEST(test_compaction_multiple_levels);
    RUN_TEST(test_scan_after_compaction);
    RUN_TEST(test_compaction_with_overlapping_keys);

    std::cout << "\n--- Combined Tests --" << std::endl;
    RUN_TEST(test_delete_persists_after_compaction);
    RUN_TEST(test_delete_and_compact);
    RUN_TEST(test_reopen_after_compaction);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
