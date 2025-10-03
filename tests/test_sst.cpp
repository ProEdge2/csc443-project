#include "test_framework.h"
#include "../src/core/database.h"
#include <string>

void test_sst_create_and_get() {
    SST<int, int> sst("test_sst_create_and_get.sst");

    std::vector<std::pair<int, int>> data = {
        {1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}
    };

    ASSERT_TRUE(sst.create_from_memtable("test_sst_create_and_get.sst", data));

    int value;
    ASSERT_TRUE(sst.get(3, value));
    ASSERT_EQUAL(300, value);

    ASSERT_FALSE(sst.get(6, value));
}

void test_sst_scan() {
    SST<int, int> sst("test_sst_scan.sst");

    std::vector<std::pair<int, int>> data = {
        {10, 1000}, {20, 2000}, {30, 3000}, {40, 4000}, {50, 5000}
    };

    ASSERT_TRUE(sst.create_from_memtable("test_sst_scan.sst", data));

    auto results = sst.scan(15, 35);
    ASSERT_EQUAL(2, static_cast<int>(results.size()));
    ASSERT_EQUAL(20, results[0].first);
    ASSERT_EQUAL(2000, results[0].second);
    ASSERT_EQUAL(30, results[1].first);
    ASSERT_EQUAL(3000, results[1].second);

    results = sst.scan(5, 9);
    ASSERT_EQUAL(0, static_cast<int>(results.size()));

    results = sst.scan(45, 60);
    ASSERT_EQUAL(1, static_cast<int>(results.size()));
    ASSERT_EQUAL(50, results[0].first);
    ASSERT_EQUAL(5000, results[0].second);
}

void test_sst_load_existing_sst() {
    SST<int, int> sst_original("test_sst_load_existing_sst.sst");

    std::vector<std::pair<int, int>> data = {
        {1, 111}, {2, 222}, {3, 333}
    };

    ASSERT_TRUE(sst_original.create_from_memtable("test_sst_load_existing_sst.sst", data));

    std::unique_ptr<SST<int, int>> sst_loaded;
    bool loaded = SST<int, int>::load_existing_sst("test_sst_load_existing_sst.sst", sst_loaded);
    ASSERT_TRUE(loaded);

    ASSERT_EQUAL(1, sst_loaded->get_min_key());
    ASSERT_EQUAL(3, sst_loaded->get_max_key());
    ASSERT_EQUAL(3, static_cast<int>(sst_loaded->get_entry_count()));

    int value;
    ASSERT_TRUE(sst_loaded->get(2, value));
    ASSERT_EQUAL(222, value);

    ASSERT_FALSE(sst_loaded->get(4, value));
}

int main() {
    std::cout << "Running SST Tests" << std::endl;

    RUN_TEST(test_sst_create_and_get);
    RUN_TEST(test_sst_scan);
    RUN_TEST(test_sst_load_existing_sst);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
