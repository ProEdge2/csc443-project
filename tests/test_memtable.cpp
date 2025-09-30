#include "test_framework.h"
#include "../src/memtable/memtable.h"
#include <string>

void test_basic_insertion() {
    RedBlackTree<int, std::string> tree(10);

    ASSERT_TRUE(tree.put(5, "five"));
    ASSERT_EQUAL(1, static_cast<int>(tree.size()));
    ASSERT_FALSE(tree.is_full());
}

void test_basic_retrieval() {
    RedBlackTree<int, std::string> tree(10);
    tree.put(5, "five");
    tree.put(3, "three");
    tree.put(7, "seven");

    std::string value;
    ASSERT_TRUE(tree.get(5, value));
    ASSERT_EQUAL(std::string("five"), value);

    ASSERT_TRUE(tree.get(3, value));
    ASSERT_EQUAL(std::string("three"), value);

    ASSERT_TRUE(tree.get(7, value));
    ASSERT_EQUAL(std::string("seven"), value);

    ASSERT_FALSE(tree.get(10, value));
}

void test_update_existing_key() {
    RedBlackTree<int, std::string> tree(10);
    tree.put(5, "five");
    ASSERT_EQUAL(1, static_cast<int>(tree.size()));

    tree.put(5, "FIVE");
    ASSERT_EQUAL(1, static_cast<int>(tree.size()));

    std::string value;
    ASSERT_TRUE(tree.get(5, value));
    ASSERT_EQUAL(std::string("FIVE"), value);
}

void test_memtable_size_limit() {
    RedBlackTree<int, std::string> tree(3);

    ASSERT_TRUE(tree.put(1, "one"));
    ASSERT_TRUE(tree.put(2, "two"));
    ASSERT_TRUE(tree.put(3, "three"));
    ASSERT_TRUE(tree.is_full());

    ASSERT_FALSE(tree.put(4, "four"));
    ASSERT_EQUAL(3, static_cast<int>(tree.size()));
}

void test_large_insertion_sequence() {
    RedBlackTree<int, int> tree(100);

    for (int i = 1; i <= 50; i++) {
        ASSERT_TRUE(tree.put(i, i * 10));
    }

    ASSERT_EQUAL(50, static_cast<int>(tree.size()));

    for (int i = 1; i <= 50; i++) {
        int value;
        ASSERT_TRUE(tree.get(i, value));
        ASSERT_EQUAL(i * 10, value);
    }
}

void test_reverse_insertion_sequence() {
    RedBlackTree<int, int> tree(100);

    for (int i = 50; i >= 1; i--) {
        ASSERT_TRUE(tree.put(i, i * 10));
    }

    ASSERT_EQUAL(50, static_cast<int>(tree.size()));

    for (int i = 1; i <= 50; i++) {
        int value;
        ASSERT_TRUE(tree.get(i, value));
        ASSERT_EQUAL(i * 10, value);
    }
}

void test_mixed_insertion_sequence() {
    RedBlackTree<int, int> tree(100);
    int keys[] = {15, 10, 20, 8, 12, 25, 6, 11, 13, 22, 27};
    int num_keys = sizeof(keys) / sizeof(keys[0]);

    for (int i = 0; i < num_keys; i++) {
        ASSERT_TRUE(tree.put(keys[i], keys[i] * 2));
    }

    ASSERT_EQUAL(num_keys, static_cast<int>(tree.size()));

    for (int i = 0; i < num_keys; i++) {
        int value;
        ASSERT_TRUE(tree.get(keys[i], value));
        ASSERT_EQUAL(keys[i] * 2, value);
    }
}

void test_string_keys() {
    RedBlackTree<std::string, int> tree(10);

    ASSERT_TRUE(tree.put("apple", 1));
    ASSERT_TRUE(tree.put("banana", 2));
    ASSERT_TRUE(tree.put("cherry", 3));

    int value;
    ASSERT_TRUE(tree.get("apple", value));
    ASSERT_EQUAL(1, value);

    ASSERT_TRUE(tree.get("banana", value));
    ASSERT_EQUAL(2, value);

    ASSERT_TRUE(tree.get("cherry", value));
    ASSERT_EQUAL(3, value);

    ASSERT_FALSE(tree.get("date", value));
}

void test_clear_functionality() {
    RedBlackTree<int, std::string> tree(10);

    tree.put(1, "one");
    tree.put(2, "two");
    tree.put(3, "three");
    ASSERT_EQUAL(3, static_cast<int>(tree.size()));

    tree.clear();
    ASSERT_EQUAL(0, static_cast<int>(tree.size()));
    ASSERT_FALSE(tree.is_full());

    std::string value;
    ASSERT_FALSE(tree.get(1, value));
    ASSERT_FALSE(tree.get(2, value));
    ASSERT_FALSE(tree.get(3, value));
}

void test_empty_tree() {
    RedBlackTree<int, std::string> tree(10);

    ASSERT_EQUAL(0, static_cast<int>(tree.size()));
    ASSERT_FALSE(tree.is_full());

    std::string value;
    ASSERT_FALSE(tree.get(1, value));
}

void test_single_element() {
    RedBlackTree<int, std::string> tree(1);

    ASSERT_TRUE(tree.put(42, "answer"));
    ASSERT_TRUE(tree.is_full());
    ASSERT_EQUAL(1, static_cast<int>(tree.size()));

    std::string value;
    ASSERT_TRUE(tree.get(42, value));
    ASSERT_EQUAL(std::string("answer"), value);

    ASSERT_FALSE(tree.put(43, "other"));
}

void test_duplicate_keys_different_values() {
    RedBlackTree<int, std::string> tree(10);

    ASSERT_TRUE(tree.put(1, "first"));
    ASSERT_TRUE(tree.put(1, "second"));
    ASSERT_TRUE(tree.put(1, "third"));

    ASSERT_EQUAL(1, static_cast<int>(tree.size()));

    std::string value;
    ASSERT_TRUE(tree.get(1, value));
    ASSERT_EQUAL(std::string("third"), value);
}

void test_red_black_properties_basic() {
    RedBlackTree<int, int> tree(20);

    tree.put(10, 10);
    tree.put(5, 5);
    tree.put(15, 15);
    tree.put(3, 3);
    tree.put(7, 7);

    ASSERT_TRUE(tree.verify_red_black_properties());
    ASSERT_TRUE(tree.get_black_height(tree.get_root()) > 0);
}

void test_red_black_properties_sequential_insertion() {
    RedBlackTree<int, int> tree(50);

    for (int i = 1; i <= 20; i++) {
        tree.put(i, i);
        ASSERT_TRUE(tree.verify_red_black_properties());
    }

    int height = tree.get_tree_height();
    ASSERT_TRUE(height <= 2 * tree.get_black_height(tree.get_root()));
}

void test_red_black_properties_reverse_insertion() {
    RedBlackTree<int, int> tree(50);

    for (int i = 20; i >= 1; i--) {
        tree.put(i, i);
        ASSERT_TRUE(tree.verify_red_black_properties());
    }

    int height = tree.get_tree_height();
    ASSERT_TRUE(height <= 2 * tree.get_black_height(tree.get_root()));
}

void test_red_black_balanced_height() {
    RedBlackTree<int, int> tree(100);

    for (int i = 1; i <= 63; i++) {
        tree.put(i, i);
    }

    int height = tree.get_tree_height();
    int black_height = tree.get_black_height(tree.get_root());

    ASSERT_TRUE(height <= 2 * black_height);
    ASSERT_TRUE(height >= black_height);
    ASSERT_TRUE(height <= 12);
}

void test_red_black_root_always_black() {
    RedBlackTree<int, int> tree(20);

    tree.put(5, 5);
    ASSERT_EQUAL(BLACK, tree.get_root()->color);

    tree.put(3, 3);
    tree.put(7, 7);
    tree.put(1, 1);
    tree.put(9, 9);

    ASSERT_EQUAL(BLACK, tree.get_root()->color);
}

void test_memtable_scan_basic() {
    RedBlackTree<std::string, int> tree(10);

    // Insert test data
    tree.put("apple", 1);
    tree.put("banana", 2);
    tree.put("cherry", 3);
    tree.put("date", 4);
    tree.put("elderberry", 5);

    // Test basic scan
    auto results = tree.scan("banana", "date");
    ASSERT_EQUAL(3, static_cast<int>(results.size()));

    // Check results are in order
    ASSERT_EQUAL(std::string("banana"), results[0].first);
    ASSERT_EQUAL(2, results[0].second);
    ASSERT_EQUAL(std::string("cherry"), results[1].first);
    ASSERT_EQUAL(3, results[1].second);
    ASSERT_EQUAL(std::string("date"), results[2].first);
    ASSERT_EQUAL(4, results[2].second);
}

void test_memtable_scan_empty_range() {
    RedBlackTree<std::string, int> tree(10);

    tree.put("apple", 1);
    tree.put("banana", 2);
    tree.put("cherry", 3);

    // Test scan with no matching keys
    auto results = tree.scan("zebra", "zoo");
    ASSERT_EQUAL(0, static_cast<int>(results.size()));
}

void test_memtable_scan_partial_range() {
    RedBlackTree<std::string, int> tree(10);

    tree.put("apple", 1);
    tree.put("banana", 2);
    tree.put("cherry", 3);
    tree.put("date", 4);
    tree.put("elderberry", 5);

    // Test scan with partial range
    auto results = tree.scan("banana", "cherry");
    ASSERT_EQUAL(2, static_cast<int>(results.size()));

    ASSERT_EQUAL(std::string("banana"), results[0].first);
    ASSERT_EQUAL(2, results[0].second);
    ASSERT_EQUAL(std::string("cherry"), results[1].first);
    ASSERT_EQUAL(3, results[1].second);
}

void test_memtable_scan_full_range() {
    RedBlackTree<std::string, int> tree(10);

    tree.put("apple", 1);
    tree.put("banana", 2);
    tree.put("cherry", 3);

    // Test scan with full range (all keys)
    auto results = tree.scan("apple", "cherry");
    ASSERT_EQUAL(3, static_cast<int>(results.size()));

    ASSERT_EQUAL(std::string("apple"), results[0].first);
    ASSERT_EQUAL(1, results[0].second);
    ASSERT_EQUAL(std::string("banana"), results[1].first);
    ASSERT_EQUAL(2, results[1].second);
    ASSERT_EQUAL(std::string("cherry"), results[2].first);
    ASSERT_EQUAL(3, results[2].second);
}

void test_memtable_scan_single_key() {
    RedBlackTree<std::string, int> tree(10);

    tree.put("apple", 1);
    tree.put("banana", 2);
    tree.put("cherry", 3);

    // Test scan with single key
    auto results = tree.scan("banana", "banana");
    ASSERT_EQUAL(1, static_cast<int>(results.size()));

    ASSERT_EQUAL(std::string("banana"), results[0].first);
    ASSERT_EQUAL(2, results[0].second);
}

void test_memtable_scan_integer_keys() {
    RedBlackTree<int, std::string> tree(10);

    tree.put(1, "one");
    tree.put(2, "two");
    tree.put(3, "three");
    tree.put(4, "four");
    tree.put(5, "five");

    // Test scan with integer keys
    auto results = tree.scan(2, 4);
    ASSERT_EQUAL(3, static_cast<int>(results.size()));

    ASSERT_EQUAL(2, results[0].first);
    ASSERT_EQUAL(std::string("two"), results[0].second);
    ASSERT_EQUAL(3, results[1].first);
    ASSERT_EQUAL(std::string("three"), results[1].second);
    ASSERT_EQUAL(4, results[2].first);
    ASSERT_EQUAL(std::string("four"), results[2].second);
}

void test_memtable_scan_empty_tree() {
    RedBlackTree<std::string, int> tree(10);

    // Test scan on empty tree
    auto results = tree.scan("apple", "banana");
    ASSERT_EQUAL(0, static_cast<int>(results.size()));
}

int main() {
    std::cout << "Running Red-Black Tree Memtable Tests" << std::endl;

    RUN_TEST(test_basic_insertion);
    RUN_TEST(test_basic_retrieval);
    RUN_TEST(test_update_existing_key);
    RUN_TEST(test_memtable_size_limit);
    RUN_TEST(test_large_insertion_sequence);
    RUN_TEST(test_reverse_insertion_sequence);
    RUN_TEST(test_mixed_insertion_sequence);
    RUN_TEST(test_string_keys);
    RUN_TEST(test_clear_functionality);
    RUN_TEST(test_empty_tree);
    RUN_TEST(test_single_element);
    RUN_TEST(test_duplicate_keys_different_values);
    RUN_TEST(test_red_black_properties_basic);
    RUN_TEST(test_red_black_properties_sequential_insertion);
    RUN_TEST(test_red_black_properties_reverse_insertion);
    RUN_TEST(test_red_black_balanced_height);
    RUN_TEST(test_red_black_root_always_black);

    // Scan functionality tests
    RUN_TEST(test_memtable_scan_basic);
    RUN_TEST(test_memtable_scan_empty_range);
    RUN_TEST(test_memtable_scan_partial_range);
    RUN_TEST(test_memtable_scan_full_range);
    RUN_TEST(test_memtable_scan_single_key);
    RUN_TEST(test_memtable_scan_integer_keys);
    RUN_TEST(test_memtable_scan_empty_tree);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
