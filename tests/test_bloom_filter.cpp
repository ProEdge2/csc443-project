#include "test_framework.h"
#include "../src/filter/bloom_filter.h"
#include <string>
#include <vector>
#include <numeric>

void test_bloom_filter_add_contains() {
    // expect 100 elements, 1% FPR
    BloomFilter<int> bf(100, 0.01);

    bf.add(10);
    bf.add(20);
    bf.add(30);

    ASSERT_TRUE(bf.contains(10));
    ASSERT_TRUE(bf.contains(20));
    ASSERT_TRUE(bf.contains(30));

    // should not contain
    ASSERT_FALSE(bf.contains(15));
    ASSERT_FALSE(bf.contains(25));
}

void test_bloom_filter_max_false_positive_rate() {
    BloomFilter<int> bf(50, 1);

    bf.add(1);
    bf.add(100);

    // everything should be true since fpr=100%
    ASSERT_TRUE(bf.contains(1));
    ASSERT_TRUE(bf.contains(100));
    ASSERT_TRUE(bf.contains(500));
    ASSERT_TRUE(bf.contains(1000));
}

void test_bloom_filter_empty() {
    BloomFilter<int> bf(10, 0.01);

    ASSERT_FALSE(bf.contains(5));
}

void test_bloom_filter_multiple_adds() {
    const size_t num_elements = 1000;
    BloomFilter<int> bf(num_elements, 0.01);

    for (size_t i = 0; i < num_elements; i++) {
        bf.add(i);
    }

    for (size_t i = 0; i < num_elements; i++) {
        ASSERT_TRUE(bf.contains(i));
    }

    // check some elements that were not added
    ASSERT_FALSE(bf.contains(num_elements + 1));
    ASSERT_FALSE(bf.contains(num_elements + 100));
}

void test_bloom_filter_false_positive_rate() {
    const size_t expected_elements = 1000;
    BloomFilter<int> bf(expected_elements, 0.01);

    // add to filter
    for (size_t i = 0; i < expected_elements; i++) {
        bf.add(i);
    }

    // verify filter contains items
    for (size_t i = 0; i < expected_elements; i++) {
        ASSERT_TRUE(bf.contains(i));
    }

    // calculate false positive rate
    size_t false_positives = 0;

    // check elements not in filter and count false positives
    for (size_t i = expected_elements; i < expected_elements * 10; i++) {
        if (bf.contains(i)) {
            false_positives++;
        }
    }

    // checked total of 9 * expected_elements number of numbers not in filter
    size_t total_checks = expected_elements * 9;
    double false_positive_rate = double(false_positives) / total_checks;

    // check false positive rate is around vicinity of expected
    ASSERT_TRUE(false_positive_rate < (0.01 * 2));
}


int main() {
    std::cout << "Running Bloom Filter Tests" << std::endl;

    RUN_TEST(test_bloom_filter_add_contains);
    RUN_TEST(test_bloom_filter_max_false_positive_rate);
    RUN_TEST(test_bloom_filter_empty);
    RUN_TEST(test_bloom_filter_multiple_adds);
    RUN_TEST(test_bloom_filter_false_positive_rate);

    TestFramework::print_results();

    return TestFramework::tests_run == TestFramework::tests_passed ? 0 : 1;
}
