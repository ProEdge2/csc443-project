#include "test_framework.h"

int TestFramework::tests_run = 0;
int TestFramework::tests_passed = 0;
std::vector<std::string> TestFramework::failed_tests;

void TestFramework::assert_true(bool condition, const std::string& test_name) {
    tests_run++;
    if (condition) {
        tests_passed++;
        std::cout << "PASS: " << test_name << std::endl;
    } else {
        failed_tests.push_back(test_name);
        std::cout << "FAIL: " << test_name << std::endl;
    }
}

void TestFramework::assert_false(bool condition, const std::string& test_name) {
    assert_true(!condition, test_name);
}

void TestFramework::assert_equal(int expected, int actual, const std::string& test_name) {
    tests_run++;
    if (expected == actual) {
        tests_passed++;
        std::cout << "PASS: " << test_name << std::endl;
    } else {
        failed_tests.push_back(test_name + " (expected: " + std::to_string(expected) + ", got: " + std::to_string(actual) + ")");
        std::cout << "FAIL: " << test_name << " (expected: " << expected << ", got: " << actual << ")" << std::endl;
    }
}

void TestFramework::assert_equal(const std::string& expected, const std::string& actual, const std::string& test_name) {
    tests_run++;
    if (expected == actual) {
        tests_passed++;
        std::cout << "PASS: " << test_name << std::endl;
    } else {
        failed_tests.push_back(test_name + " (expected: " + expected + ", got: " + actual + ")");
        std::cout << "FAIL: " << test_name << " (expected: " << expected << ", got: " << actual << ")" << std::endl;
    }
}

void TestFramework::run_test(void (*test_func)(), const std::string& test_name) {
    std::cout << "\n--- Running " << test_name << " ---" << std::endl;
    test_func();
}

void TestFramework::print_results() {
    std::cout << "\n== Test Results ==" << std::endl;
    std::cout << "Tests run: " << tests_run << std::endl;
    std::cout << "Tests passed: " << tests_passed << std::endl;
    std::cout << "Tests failed: " << (tests_run - tests_passed) << std::endl;

    if (!failed_tests.empty()) {
        std::cout << "\nFailed tests:" << std::endl;
        for (const auto& test : failed_tests) {
            std::cout << "  - " << test << std::endl;
        }
    }

    std::cout << "\nSuccess rate: " << (tests_run > 0 ? (tests_passed * 100.0 / tests_run) : 0) << "%" << std::endl;
}

void TestFramework::reset() {
    tests_run = 0;
    tests_passed = 0;
    failed_tests.clear();
}
