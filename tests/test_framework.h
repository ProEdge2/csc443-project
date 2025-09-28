#ifndef TEST_FRAMEWORK_H
#define TEST_FRAMEWORK_H

#include <iostream>
#include <string>
#include <vector>

class TestFramework {
public:
    static int tests_run;
    static int tests_passed;
    static std::vector<std::string> failed_tests;
    static void assert_true(bool condition, const std::string& test_name);
    static void assert_false(bool condition, const std::string& test_name);
    static void assert_equal(int expected, int actual, const std::string& test_name);
    static void assert_equal(const std::string& expected, const std::string& actual, const std::string& test_name);
    static void run_test(void (*test_func)(), const std::string& test_name);
    static void print_results();
    static void reset();
};

#define ASSERT_TRUE(condition) TestFramework::assert_true(condition, __func__)
#define ASSERT_FALSE(condition) TestFramework::assert_false(condition, __func__)
#define ASSERT_EQUAL(expected, actual) TestFramework::assert_equal(expected, actual, __func__)
#define RUN_TEST(test_func) TestFramework::run_test(test_func, #test_func)

#endif
