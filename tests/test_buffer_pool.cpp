#include "test_framework.h"
#include "../src/buffer/buffer_pool.h"
#include <cstring>
#include <iostream>

void test_page_id_equality() {
    PageID page1("file1.sst", 0);
    PageID page2("file1.sst", 0);
    PageID page3("file1.sst", 4096);
    PageID page4("file2.sst", 0);

    ASSERT_TRUE(page1 == page2);
    ASSERT_FALSE(page1 == page3);
    ASSERT_FALSE(page1 == page4);
    ASSERT_TRUE(page1 != page3);
}

void test_buffer_pool_initialization() {
    BufferPool pool(2, 5, 4, 1000);
    ASSERT_EQUAL(4, static_cast<int>(pool.get_directory_size()));
    ASSERT_EQUAL(2, static_cast<int>(pool.get_global_depth()));
    ASSERT_EQUAL(0, static_cast<int>(pool.get_page_count()));
    ASSERT_EQUAL(1000, static_cast<int>(pool.get_max_pages()));
    ASSERT_FALSE(pool.is_full());
}

void test_put_and_get_single_page() {
    BufferPool pool(2, 5, 10, 100);
    PageID page_id("test.sst", 0);

    char write_data[PAGE_SIZE];
    for (size_t i = 0; i < PAGE_SIZE; i++) {
        write_data[i] = static_cast<char>(i % 256);
    }

    ASSERT_TRUE(pool.put_page(page_id, write_data));
    ASSERT_EQUAL(1, static_cast<int>(pool.get_page_count()));

    char read_data[PAGE_SIZE];
    ASSERT_TRUE(pool.get_page(page_id, read_data));

    bool data_matches = true;
    for (size_t i = 0; i < PAGE_SIZE; i++) {
        if (read_data[i] != write_data[i]) {
            data_matches = false;
            break;
        }
    }
    ASSERT_TRUE(data_matches);
}

void test_put_multiple_pages() {
    BufferPool pool(2, 5, 10, 100);

    for (int i = 0; i < 50; i++) {
        PageID page_id("test.sst", i * PAGE_SIZE);
        char data[PAGE_SIZE];
        std::memset(data, i, PAGE_SIZE);

        ASSERT_TRUE(pool.put_page(page_id, data));
    }

    ASSERT_EQUAL(50, static_cast<int>(pool.get_page_count()));


    for (int i = 0; i < 50; i++) {
        PageID page_id("test.sst", i * PAGE_SIZE);
        ASSERT_TRUE(pool.contains_page(page_id));
    }
}

void test_update_existing_page() {
    BufferPool pool(2, 5, 10, 100);
    PageID page_id("test.sst", 0);

    char data1[PAGE_SIZE];
    std::memset(data1, 1, PAGE_SIZE);
    ASSERT_TRUE(pool.put_page(page_id, data1));
    ASSERT_EQUAL(1, static_cast<int>(pool.get_page_count()));

    char data2[PAGE_SIZE];
    std::memset(data2, 2, PAGE_SIZE);
    ASSERT_TRUE(pool.put_page(page_id, data2));
    ASSERT_EQUAL(1, static_cast<int>(pool.get_page_count()));

    char read_data[PAGE_SIZE];
    ASSERT_TRUE(pool.get_page(page_id, read_data));

    bool all_match = true;
    for (size_t i = 0; i < PAGE_SIZE; i++) {
        if (read_data[i] != 2) {
            all_match = false;


            break;
        }
    }

    ASSERT_TRUE(all_match);
}

void test_remove_page() {
    BufferPool pool(2, 5, 10, 100);
    PageID page_id("test.sst", 0);

    char data[PAGE_SIZE];
    std::memset(data, 1, PAGE_SIZE);
    ASSERT_TRUE(pool.put_page(page_id, data));
    ASSERT_EQUAL(1, static_cast<int>(pool.get_page_count()));
    ASSERT_TRUE(pool.contains_page(page_id));

    ASSERT_TRUE(pool.remove_page(page_id));
    ASSERT_EQUAL(0, static_cast<int>(pool.get_page_count()));
    ASSERT_FALSE(pool.contains_page(page_id));
}

void test_buffer_pool_full() {
    BufferPool pool(2, 5, 5, 10);

    for (int i = 0; i < 10; i++) {
        PageID page_id("file" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        std::memset(data, i, PAGE_SIZE);
        ASSERT_TRUE(pool.put_page(page_id, data));
    }

    ASSERT_TRUE(pool.is_full());
    ASSERT_EQUAL(10, static_cast<int>(pool.get_page_count()));

    PageID extra_page("extra.sst", 0);
    char data[PAGE_SIZE];
    std::memset(data, 99, PAGE_SIZE);
    ASSERT_FALSE(pool.put_page(extra_page, data));
    ASSERT_EQUAL(10, static_cast<int>(pool.get_page_count()));
}

void test_clear_buffer_pool() {
    BufferPool pool(2, 5, 10, 100);

    for (int i = 0; i < 20; i++) {
        PageID page_id("file" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        std::memset(data, i, PAGE_SIZE);
        pool.put_page(page_id, data);
    }

    ASSERT_EQUAL(20, static_cast<int>(pool.get_page_count()));

    pool.clear();
    ASSERT_EQUAL(0, static_cast<int>(pool.get_page_count()));

    for (int i = 0; i < 20; i++) {
        PageID page_id("file" + std::to_string(i) + ".sst", 0);
        ASSERT_FALSE(pool.contains_page(page_id));
    }
}

void test_different_files_same_offset() {
    BufferPool pool(2, 5, 10, 100);

    PageID page1("file1.sst", 0);
    PageID page2("file2.sst", 0);
    PageID page3("file3.sst", 0);

    char data1[PAGE_SIZE], data2[PAGE_SIZE], data3[PAGE_SIZE];
    std::memset(data1, 1, PAGE_SIZE);
    std::memset(data2, 2, PAGE_SIZE);
    std::memset(data3, 3, PAGE_SIZE);

    ASSERT_TRUE(pool.put_page(page1, data1));
    ASSERT_TRUE(pool.put_page(page2, data2));
    ASSERT_TRUE(pool.put_page(page3, data3));

    char read_data[PAGE_SIZE];
    ASSERT_TRUE(pool.get_page(page1, read_data));
    ASSERT_TRUE(read_data[0] == 1);

    ASSERT_TRUE(pool.get_page(page2, read_data));
    ASSERT_TRUE(read_data[0] == 2);

    ASSERT_TRUE(pool.get_page(page3, read_data));
    ASSERT_TRUE(read_data[0] == 3);
}


void test_same_file_different_offsets() {
    BufferPool pool(2, 5, 10, 100);

    PageID page1("file.sst", 0);
    PageID page2("file.sst", PAGE_SIZE);
    PageID page3("file.sst", PAGE_SIZE * 2);

    char data1[PAGE_SIZE], data2[PAGE_SIZE], data3[PAGE_SIZE];
    std::memset(data1, 10, PAGE_SIZE);
    std::memset(data2, 20, PAGE_SIZE);
    std::memset(data3, 30, PAGE_SIZE);

    ASSERT_TRUE(pool.put_page(page1, data1));
    ASSERT_TRUE(pool.put_page(page2, data2));
    ASSERT_TRUE(pool.put_page(page3, data3));

    char read_data[PAGE_SIZE];
    ASSERT_TRUE(pool.get_page(page1, read_data));
    ASSERT_TRUE(read_data[0] == 10);

    ASSERT_TRUE(pool.get_page(page2, read_data));
    ASSERT_TRUE(read_data[0] == 20);

    ASSERT_TRUE(pool.get_page(page3, read_data));
    ASSERT_TRUE(read_data[0] == 30);
}

void test_get_nonexistent_page() {
    BufferPool pool(2, 5, 10, 100);
    PageID page_id("nonexistent.sst", 0);

    char data[PAGE_SIZE];
    ASSERT_FALSE(pool.get_page(page_id, data));
    ASSERT_FALSE(pool.contains_page(page_id));
}

void test_remove_nonexistent_page() {
    BufferPool pool(2, 5, 10, 100);
    PageID page_id("nonexistent.sst", 0);

    ASSERT_FALSE(pool.remove_page(page_id));
    ASSERT_EQUAL(0, static_cast<int>(pool.get_page_count()));
}

void test_directory_expansion() {
    BufferPool pool(1, 5, 2, 100);

    size_t initial_dir_size = pool.get_directory_size();
    ASSERT_EQUAL(2, static_cast<int>(initial_dir_size));
    ASSERT_EQUAL(1, static_cast<int>(pool.get_global_depth()));

    for (int i = 0; i < 10; i++) {
        PageID page_id("file" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        std::memset(data, i, PAGE_SIZE);
        ASSERT_TRUE(pool.put_page(page_id, data));
    }

    ASSERT_TRUE(pool.get_directory_size() >= initial_dir_size);
    ASSERT_TRUE(pool.get_global_depth() >= 1);
}

void test_bucket_splitting() {
    BufferPool pool(2, 6, 3, 100);

    size_t initial_depth = pool.get_global_depth();

    for (int i = 0; i < 20; i++) {
        PageID page_id("bucket_test_" + std::to_string(i) + ".sst", i * PAGE_SIZE);
        char data[PAGE_SIZE];
        std::memset(data, i % 256, PAGE_SIZE);
        ASSERT_TRUE(pool.put_page(page_id, data));
    }

    for (int i = 0; i < 20; i++) {
        PageID page_id("bucket_test_" + std::to_string(i) + ".sst", i * PAGE_SIZE);
        ASSERT_TRUE(pool.contains_page(page_id));
    }

    ASSERT_EQUAL(20, static_cast<int>(pool.get_page_count()));
}

void test_max_depth_limit() {
    BufferPool pool(1, 3, 2, 100);

    for (int i = 0; i < 50; i++) {
        PageID page_id("file" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        std::memset(data, i % 256, PAGE_SIZE);
        pool.put_page(page_id, data);
    }

    ASSERT_TRUE(pool.get_global_depth() <= 3);
    ASSERT_TRUE(pool.get_directory_size() <= 8);
}

void test_stress_expandable() {
    BufferPool pool(2, 8, 5, 200);

    for (int i = 0; i < 150; i++) {
        PageID page_id("stress_" + std::to_string(i / 10) + ".sst", (i % 10) * PAGE_SIZE);
        char data[PAGE_SIZE];
        std::memset(data, i % 256, PAGE_SIZE);
        ASSERT_TRUE(pool.put_page(page_id, data));
    }

    ASSERT_EQUAL(150, static_cast<int>(pool.get_page_count()));

    for (int i = 0; i < 150; i++) {
        PageID page_id("stress_" + std::to_string(i / 10) + ".sst", (i % 10) * PAGE_SIZE);
        char data[PAGE_SIZE];
        ASSERT_TRUE(pool.get_page(page_id, data));
        ASSERT_TRUE(data[0] == static_cast<char>(i % 256));
    }
}

void test_pages_persist_after_split() {
    BufferPool pool(1, 5, 2, 50);

    for (int i = 0; i < 10; i++) {
        PageID page_id("persist_" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        std::memset(data, i + 100, PAGE_SIZE);
        ASSERT_TRUE(pool.put_page(page_id, data));
    }

    for (int i = 0; i < 10; i++) {
        PageID page_id("persist_" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        ASSERT_TRUE(pool.get_page(page_id, data));
        ASSERT_TRUE(data[0] == static_cast<char>(i + 100));
    }
}

void test_mixed_operations_expandable() {
    BufferPool pool(2, 6, 4, 100);

    for (int i = 0; i < 30; i++) {
        PageID page_id("mixed_" + std::to_string(i) + ".sst", 0);
        char data[PAGE_SIZE];
        std::memset(data, i, PAGE_SIZE);
        pool.put_page(page_id, data);
    }

    for (int i = 0; i < 10; i++) {
        PageID page_id("mixed_" + std::to_string(i) + ".sst", 0);
        pool.remove_page(page_id);
    }

    ASSERT_EQUAL(20, static_cast<int>(pool.get_page_count()));

    for (int i = 10; i < 30; i++) {
        PageID page_id("mixed_" + std::to_string(i) + ".sst", 0);
        ASSERT_TRUE(pool.contains_page(page_id));
    }

    for (int i = 0; i < 10; i++) {
        PageID page_id("mixed_" + std::to_string(i) + ".sst", 0);
        ASSERT_FALSE(pool.contains_page(page_id));
    }
}

int main() {
    TestFramework::reset();

    std::cout << "\n=== Running Extendible Hashing Buffer Pool Tests ===" << std::endl;

    RUN_TEST(test_page_id_equality);
    RUN_TEST(test_buffer_pool_initialization);
    RUN_TEST(test_put_and_get_single_page);
    RUN_TEST(test_put_multiple_pages);
    RUN_TEST(test_update_existing_page);
    RUN_TEST(test_remove_page);
    RUN_TEST(test_buffer_pool_full);
    RUN_TEST(test_clear_buffer_pool);
    RUN_TEST(test_different_files_same_offset);
    RUN_TEST(test_same_file_different_offsets);
    RUN_TEST(test_get_nonexistent_page);
    RUN_TEST(test_remove_nonexistent_page);

    std::cout << "\n=  Extendible Hashing Specific Tests   =" << std::endl;
    RUN_TEST(test_directory_expansion);
    RUN_TEST(test_bucket_splitting);
    RUN_TEST(test_max_depth_limit);
    RUN_TEST(test_stress_expandable);
    RUN_TEST(test_pages_persist_after_split);
    RUN_TEST(test_mixed_operations_expandable);

    TestFramework::print_results();

    return TestFramework::tests_passed == TestFramework::tests_run ? 0 : 1;
}
