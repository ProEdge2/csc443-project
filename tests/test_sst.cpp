#include "test_framework.h"
#include "../src/core/database.h"
#include "../src/storage/sst.h"
#include <string>
#include <filesystem>

// Set up test directory
std::string setup_test_directory(const std::string& test_name) {
    const std::string test_dir = "test_output/sst_tests/" + test_name;
    std::filesystem::remove_all(test_dir);
    std::filesystem::create_directories(test_dir);
    return test_dir;
}

void test_sst_create_and_get_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_create_and_get_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data = {
        {1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}
    };

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(3, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(300, value);

    ASSERT_FALSE(sst.get(6, value, SearchMode::B_TREE_SEARCH));
}

void test_sst_create_and_get_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_create_and_get_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data = {
        {1, 100}, {2, 200}, {3, 300}, {4, 400}, {5, 500}
    };

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(3, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(300, value);

    ASSERT_FALSE(sst.get(6, value, SearchMode::BINARY_SEARCH));
}

void test_sst_scan_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_scan_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data = {
        {10, 1000}, {20, 2000}, {30, 3000}, {40, 4000}, {50, 5000}
    };

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    auto results = sst.scan(15, 35, SearchMode::B_TREE_SEARCH);
    ASSERT_EQUAL(2, static_cast<int>(results.size()));
    ASSERT_EQUAL(20, results[0].first);
    ASSERT_EQUAL(2000, results[0].second);
    ASSERT_EQUAL(30, results[1].first);
    ASSERT_EQUAL(3000, results[1].second);

    results = sst.scan(5, 9, SearchMode::B_TREE_SEARCH);
    ASSERT_EQUAL(0, static_cast<int>(results.size()));

    results = sst.scan(45, 60, SearchMode::B_TREE_SEARCH);
    ASSERT_EQUAL(1, static_cast<int>(results.size()));
    ASSERT_EQUAL(50, results[0].first);
    ASSERT_EQUAL(5000, results[0].second);
}

void test_sst_scan_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_scan_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data = {
        {10, 1000}, {20, 2000}, {30, 3000}, {40, 4000}, {50, 5000}
    };

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    auto results = sst.scan(15, 35, SearchMode::BINARY_SEARCH);
    ASSERT_EQUAL(2, static_cast<int>(results.size()));
    ASSERT_EQUAL(20, results[0].first);
    ASSERT_EQUAL(2000, results[0].second);
    ASSERT_EQUAL(30, results[1].first);
    ASSERT_EQUAL(3000, results[1].second);

    results = sst.scan(5, 9, SearchMode::BINARY_SEARCH);
    ASSERT_EQUAL(0, static_cast<int>(results.size()));

    results = sst.scan(45, 60, SearchMode::BINARY_SEARCH);
    ASSERT_EQUAL(1, static_cast<int>(results.size()));
    ASSERT_EQUAL(50, results[0].first);
    ASSERT_EQUAL(5000, results[0].second);
}

void test_sst_load_existing_sst_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_load_existing_sst_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst_original(sst_path);

    std::vector<std::pair<int, int>> data = {
        {1, 111}, {2, 222}, {3, 333}
    };

    ASSERT_TRUE(sst_original.create_from_memtable(sst_path, data));

    std::unique_ptr<SST<int, int>> sst_loaded;
    bool loaded = SST<int, int>::load_existing_sst(sst_path, sst_loaded);
    ASSERT_TRUE(loaded);

    ASSERT_EQUAL(1, sst_loaded->get_min_key());
    ASSERT_EQUAL(3, sst_loaded->get_max_key());
    ASSERT_EQUAL(3, static_cast<int>(sst_loaded->get_entry_count()));

    int value;
    ASSERT_TRUE(sst_loaded->get(2, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(222, value);

    ASSERT_FALSE(sst_loaded->get(4, value, SearchMode::B_TREE_SEARCH));
}

void test_sst_load_existing_sst_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_load_existing_sst_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst_original(sst_path);

    std::vector<std::pair<int, int>> data = {
        {1, 111}, {2, 222}, {3, 333}
    };

    ASSERT_TRUE(sst_original.create_from_memtable(sst_path, data));

    std::unique_ptr<SST<int, int>> sst_loaded;
    bool loaded = SST<int, int>::load_existing_sst(sst_path, sst_loaded);
    ASSERT_TRUE(loaded);

    ASSERT_EQUAL(1, sst_loaded->get_min_key());
    ASSERT_EQUAL(3, sst_loaded->get_max_key());
    ASSERT_EQUAL(3, static_cast<int>(sst_loaded->get_entry_count()));

    int value;
    ASSERT_TRUE(sst_loaded->get(2, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(222, value);

    ASSERT_FALSE(sst_loaded->get(4, value, SearchMode::BINARY_SEARCH));
}

void test_sst_large_dataset_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_large_dataset_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data;
    for (int i = 0; i <= 2500; i++) {
        data.push_back(std::make_pair(i, i * 10));
    }

    // Creates SST with 5 leaf nodes (512 pairs per leaf)
    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(300, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(3000, value);

    ASSERT_FALSE(sst.get(3000, value, SearchMode::B_TREE_SEARCH));

    auto results = sst.scan(56, 812, SearchMode::B_TREE_SEARCH);
    ASSERT_EQUAL(757, static_cast<int>(results.size()));
    ASSERT_EQUAL(56, results[0].first);
    ASSERT_EQUAL(560, results[0].second);
    ASSERT_EQUAL(57, results[1].first);
    ASSERT_EQUAL(570, results[1].second);
    ASSERT_EQUAL(812, results[756].first);
    ASSERT_EQUAL(8120, results[756].second);
}

void test_sst_large_dataset_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_large_dataset_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data;
    for (int i = 0; i <= 2500; i++) {
        data.push_back(std::make_pair(i, i * 10));
    }

    // Creates SST with 5 leaf nodes (512 pairs per leaf)
    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(333, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(3330, value);

    ASSERT_FALSE(sst.get(3000, value, SearchMode::BINARY_SEARCH));

    auto results = sst.scan(56, 812, SearchMode::BINARY_SEARCH);
    ASSERT_EQUAL(757, static_cast<int>(results.size()));
    ASSERT_EQUAL(56, results[0].first);
    ASSERT_EQUAL(560, results[0].second);
    ASSERT_EQUAL(57, results[1].first);
    ASSERT_EQUAL(570, results[1].second);
    ASSERT_EQUAL(812, results[756].first);
    ASSERT_EQUAL(8120, results[756].second);
}

void test_sst_many_leaves_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_many_leaves_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data;
    for (int i = 0; i < 10000; i++) {
        data.push_back(std::make_pair(i, i * 2));
    }

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    // Test get
    int value;
    ASSERT_TRUE(sst.get(5000, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(10000, value);

    ASSERT_TRUE(sst.get(0, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(0, value);

    ASSERT_TRUE(sst.get(9999, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(19998, value);

    ASSERT_FALSE(sst.get(10000, value, SearchMode::B_TREE_SEARCH));

    // Test scan
    auto results = sst.scan(4990, 5010, SearchMode::B_TREE_SEARCH);
    ASSERT_EQUAL(21, static_cast<int>(results.size()));
    for (int i = 0; i < 21; i++) {
        ASSERT_EQUAL(4990 + i, results[i].first);
        ASSERT_EQUAL((4990 + i) * 2, results[i].second);
    }
}

void test_sst_many_leaves_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_many_leaves_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);

    std::vector<std::pair<int, int>> data;
    const int num_pairs = 10000;
    for (int i = 0; i < num_pairs; i++) {
        data.push_back(std::make_pair(i, i * 2));
    }

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    // Test get
    int value;
    ASSERT_TRUE(sst.get(5000, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(10000, value);

    ASSERT_TRUE(sst.get(0, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(0, value);

    ASSERT_TRUE(sst.get(9999, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(19998, value);

    ASSERT_FALSE(sst.get(10000, value, SearchMode::BINARY_SEARCH));

    // Test scan
    auto results = sst.scan(4990, 5010, SearchMode::BINARY_SEARCH);
    ASSERT_EQUAL(21, static_cast<int>(results.size()));
    for (int i = 0; i < 21; i++) {
        ASSERT_EQUAL(4990 + i, results[i].first);
        ASSERT_EQUAL((4990 + i) * 2, results[i].second);
    }
}

void test_sst_single_pair_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_single_pair_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);
    std::vector<std::pair<int, int>> data = {{1, 100}};
    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(1, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(100, value);
    ASSERT_FALSE(sst.get(2, value, SearchMode::B_TREE_SEARCH));
}

void test_sst_single_pair_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_single_pair_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);
    std::vector<std::pair<int, int>> data = {{1, 100}};
    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(1, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(100, value);
    ASSERT_FALSE(sst.get(2, value, SearchMode::BINARY_SEARCH));
}

void test_sst_leaf_node_boundaries_b_tree() {
    // Test last pair in leaf
    {
        const std::string test_dir = setup_test_directory("test_sst_leaf_node_last_pair_b_tree");
        const std::string sst_path = test_dir + "/test.sst";

        SST<int, int> sst(sst_path);
        std::vector<std::pair<int, int>> data;
        const int num_pairs = LeafNode<int, int>::PAIRS_COUNT;
        for (int i = 0; i < num_pairs; i++) {
            data.push_back({i, i * 2});
        }

        ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

        int value;
        ASSERT_TRUE(sst.get(num_pairs - 1, value, SearchMode::B_TREE_SEARCH));
        ASSERT_EQUAL((num_pairs - 1) * 2, value);
    }

    // Test first pair in leaf
    {
        const std::string test_dir = setup_test_directory("test_sst_leaf_node_first_pair_b_tree");
        const std::string sst_path = test_dir + "/test.sst";

        SST<int, int> sst(sst_path);
        std::vector<std::pair<int, int>> data;
        const int num_pairs = LeafNode<int, int>::PAIRS_COUNT;
        for (int i = 0; i < num_pairs + 1; i++) {
            data.push_back({i, i * 2});
        }
        ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

        int value;
        ASSERT_TRUE(sst.get(num_pairs, value, SearchMode::B_TREE_SEARCH));
        ASSERT_EQUAL(num_pairs * 2, value);
    }
}

void test_sst_leaf_node_boundaries_binary_search() {
    // Test last pair in leaf
    {
        const std::string test_dir = setup_test_directory("test_sst_leaf_node_last_pair_binary_search");
        const std::string sst_path = test_dir + "/test.sst";

        SST<int, int> sst(sst_path);
        std::vector<std::pair<int, int>> data;
        const int num_pairs = LeafNode<int, int>::PAIRS_COUNT;
        for (int i = 0; i < num_pairs; i++) {
            data.push_back({i, i * 2});
        }

        ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

        int value;
        ASSERT_TRUE(sst.get(num_pairs - 1, value, SearchMode::BINARY_SEARCH));
        ASSERT_EQUAL((num_pairs - 1) * 2, value);
    }

    // Test first pair in leaf
    {
        const std::string test_dir = setup_test_directory("test_sst_leaf_node_first_pair_binary_search");
        const std::string sst_path = test_dir + "/test.sst";

        SST<int, int> sst(sst_path);
        std::vector<std::pair<int, int>> data;
        const int num_pairs = LeafNode<int, int>::PAIRS_COUNT;
        for (int i = 0; i < num_pairs + 1; i++) {
            data.push_back({i, i * 2});
        }
        ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

        int value;
        ASSERT_TRUE(sst.get(num_pairs, value, SearchMode::BINARY_SEARCH));
        ASSERT_EQUAL(num_pairs * 2, value);
    }
}

// depth 3 tree
void test_sst_deep_tree_b_tree() {
    const std::string test_dir = setup_test_directory("test_sst_deep_tree_b_tree");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);
    std::vector<std::pair<int, int>> data;
    const int num_pairs = (InternalNode<int>::MAX_KEYS + 1) * LeafNode<int, int>::PAIRS_COUNT;
    for (int i = 0; i < num_pairs; i++) {
        data.push_back({i, i * 3});
    }

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(0, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL(0, value);

    ASSERT_TRUE(sst.get(num_pairs / 2, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL((num_pairs / 2) * 3, value);

    ASSERT_TRUE(sst.get(num_pairs - 1, value, SearchMode::B_TREE_SEARCH));
    ASSERT_EQUAL((num_pairs - 1) * 3, value);
}

// depth 3 tree
void test_sst_deep_tree_binary_search() {
    const std::string test_dir = setup_test_directory("test_sst_deep_tree_binary_search");
    const std::string sst_path = test_dir + "/test.sst";

    SST<int, int> sst(sst_path);
    std::vector<std::pair<int, int>> data;
    const int num_pairs = (InternalNode<int>::MAX_KEYS + 1) * LeafNode<int, int>::PAIRS_COUNT;
    for (int i = 0; i < num_pairs; i++) {
        data.push_back({i, i * 3});
    }

    ASSERT_TRUE(sst.create_from_memtable(sst_path, data));

    int value;
    ASSERT_TRUE(sst.get(0, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL(0, value);

    ASSERT_TRUE(sst.get(num_pairs / 2, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL((num_pairs / 2) * 3, value);

    ASSERT_TRUE(sst.get(num_pairs - 1, value, SearchMode::BINARY_SEARCH));
    ASSERT_EQUAL((num_pairs - 1) * 3, value);
}

int main() {
    std::cout << "Running SST Tests" << std::endl;

    RUN_TEST(test_sst_create_and_get_b_tree);
    RUN_TEST(test_sst_create_and_get_binary_search);
    RUN_TEST(test_sst_scan_b_tree);
    RUN_TEST(test_sst_scan_binary_search);
    RUN_TEST(test_sst_load_existing_sst_b_tree);
    RUN_TEST(test_sst_load_existing_sst_binary_search);
    RUN_TEST(test_sst_large_dataset_b_tree);
    RUN_TEST(test_sst_large_dataset_binary_search);
    RUN_TEST(test_sst_many_leaves_b_tree);
    RUN_TEST(test_sst_many_leaves_binary_search);
    RUN_TEST(test_sst_single_pair_b_tree);
    RUN_TEST(test_sst_single_pair_binary_search);
    RUN_TEST(test_sst_leaf_node_boundaries_b_tree);
    RUN_TEST(test_sst_leaf_node_boundaries_binary_search);
    RUN_TEST(test_sst_deep_tree_b_tree);
    RUN_TEST(test_sst_deep_tree_binary_search);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
