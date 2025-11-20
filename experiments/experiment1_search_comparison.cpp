#include "../src/core/database.h"
#include "experiment_utils.h"
#include <iostream>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <iomanip>

constexpr size_t BUFFER_POOL_SIZE_MB = 10;
constexpr size_t BUFFER_POOL_PAGES = (BUFFER_POOL_SIZE_MB * 1024 * 1024) / 4096; // 2560 pages
constexpr size_t MEMTABLE_SIZE_ENTRIES = (1 * 1024 * 1024) / sizeof(std::pair<int, int>); // 131,072 entries
constexpr size_t BITS_PER_ENTRY = 8;
const double BLOOM_FILTER_FPR = calculate_fpr_from_bits_per_entry(BITS_PER_ENTRY);

// Data sizes to test (in MB)
const std::vector<size_t> DATA_SIZES_MB = {1, 5, 10, 25, 50, 100, 200, 500};
constexpr size_t QUERY_BATCH_SIZE = 10000; // Number of queries per measurement
constexpr size_t NUM_WARMUP_QUERIES = 1000; // Warm up buffer pool before measuring

void cleanup_database(const std::string& db_name) {
    std::string db_dir = "data/" + db_name;
    if (std::filesystem::exists(db_dir)) {
        std::filesystem::remove_all(db_dir);
    }
}

size_t calculate_entry_count_for_size_mb(size_t size_mb) {
    // Each entry is a pair<int, int> = 8 bytes
    return (size_mb * 1024 * 1024) / sizeof(std::pair<int, int>);
}

struct Experiment1Result {
    size_t data_size_mb;
    double binary_throughput;
    double btree_throughput;
};

void insert_random_data(Database<int, int>& db, size_t num_entries, RandomGenerator& rng) {
    std::cout << "Inserting " << num_entries << " entries..." << std::flush;

    // Generate unique random keys
    int max_key = std::numeric_limits<int>::max() / 2; // Avoid overflow
    std::vector<int> keys = rng.generate_unique_random_ints(num_entries, 0, max_key);

    Timer insert_timer;
    insert_timer.start();

    for (size_t i = 0; i < keys.size(); ++i) {
        db.put(keys[i], keys[i] * 10); // Value is key * 10
    }

    insert_timer.stop();
    std::cout << " Done (" << insert_timer.elapsed_seconds() << "s)" << std::endl;
}

double measure_query_throughput(Database<int, int>& db, const std::vector<int>& query_keys,
                                SearchMode mode, size_t num_queries) {
    Timer query_timer;
    query_timer.start();

    int value;
    size_t successful_queries = 0;
    for (size_t i = 0; i < num_queries; ++i) {
        if (db.get(query_keys[i % query_keys.size()], value, mode)) {
            successful_queries++;
        }
    }

    query_timer.stop();
    return calculate_throughput(num_queries, query_timer.elapsed_seconds());
}

void run_experiment_for_size(size_t data_size_mb, CSVWriter& csv_writer, RandomGenerator& rng,
                             std::vector<Experiment1Result>& summary_rows) {
    std::cout << "\n=== Testing data size: " << data_size_mb << " MB ===" << std::endl;

    size_t num_entries = calculate_entry_count_for_size_mb(data_size_mb);
    std::string db_name = "exp1_search_" + std::to_string(data_size_mb) + "mb";

    // Clean up any existing database
    cleanup_database(db_name);

    // Create and open database
    Database<int, int> db(db_name, MEMTABLE_SIZE_ENTRIES, BLOOM_FILTER_FPR, BUFFER_POOL_PAGES);
    if (!db.open()) {
        std::cerr << "Failed to open database!" << std::endl;
        return;
    }

    // Insert data
    insert_random_data(db, num_entries, rng);

    // Force flush to ensure all data is in SST files
    db.flush_memtable_to_sst();

    // Get all keys for query generation
    std::cout << "Collecting keys for queries..." << std::flush;
    std::vector<int> all_keys;
    // We'll generate random keys in the same range as inserted keys
    int max_key = std::numeric_limits<int>::max() / 2;
    std::vector<int> query_keys = rng.generate_random_ints(QUERY_BATCH_SIZE, 0, max_key);
    std::cout << " Done" << std::endl;

    // Warm up buffer pool
    std::cout << "Warming up buffer pool..." << std::flush;
    int dummy_value;
    for (size_t i = 0; i < NUM_WARMUP_QUERIES; ++i) {
        db.get(query_keys[i % query_keys.size()], dummy_value, SearchMode::B_TREE_SEARCH);
    }
    std::cout << " Done" << std::endl;

    // Measure binary search throughput
    std::cout << "Measuring binary search throughput..." << std::flush;
    double binary_throughput = measure_query_throughput(db, query_keys, SearchMode::BINARY_SEARCH, QUERY_BATCH_SIZE);
    std::cout << " " << binary_throughput << " queries/sec" << std::endl;

    // Measure B-tree search throughput
    std::cout << "Measuring B-tree search throughput..." << std::flush;
    double btree_throughput = measure_query_throughput(db, query_keys, SearchMode::B_TREE_SEARCH, QUERY_BATCH_SIZE);
    std::cout << " " << btree_throughput << " queries/sec" << std::endl;

    // Write results to CSV
    csv_writer.write_row({std::to_string(data_size_mb),
                          std::to_string(binary_throughput),
                          std::to_string(btree_throughput)});

    summary_rows.push_back({data_size_mb, binary_throughput, btree_throughput});

    // Close database
    db.close();

    // Clean up
    cleanup_database(db_name);
}

void print_summary_table(const std::vector<Experiment1Result>& rows) {
    if (rows.empty()) {
        return;
    }

    std::cout << "\nFinal throughput table (ops/sec):" << std::endl;
    std::cout << std::left << std::setw(15) << "Data Size MB"
              << std::right << std::setw(20) << "Binary Search"
              << std::right << std::setw(20) << "B-Tree Search" << std::endl;
    std::cout << std::string(55, '-') << std::endl;

    std::cout << std::fixed << std::setprecision(0);
    for (const auto& row : rows) {
        std::cout << std::left << std::setw(15) << row.data_size_mb
                  << std::right << std::setw(20) << row.binary_throughput
                  << std::right << std::setw(20) << row.btree_throughput << std::endl;
    }
    std::cout.unsetf(std::ios::floatfield);
}

int main() {
    std::cout << "=== Experiment 1: Binary Search vs B-Tree Search Throughput Comparison ===" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Buffer pool: " << BUFFER_POOL_SIZE_MB << " MB (" << BUFFER_POOL_PAGES << " pages)" << std::endl;
    std::cout << "  Memtable: 1 MB (" << MEMTABLE_SIZE_ENTRIES << " entries)" << std::endl;
    std::cout << "  Bloom filter: " << BITS_PER_ENTRY << " bits per entry (FPR ≈ " << BLOOM_FILTER_FPR << ")" << std::endl;
    std::cout << "  Query batch size: " << QUERY_BATCH_SIZE << " queries" << std::endl;
    std::cout << std::endl;

    // Ensure results directory exists
    ensure_directory_exists("experiments/results");

    // Create CSV writer
    CSVWriter csv_writer("experiments/results/experiment1_results.csv");
    csv_writer.write_header({"data_size_mb", "binary_search_throughput", "btree_search_throughput"});

    // Initialize random generator with fixed seed for reproducibility
    RandomGenerator rng(42);

    std::vector<Experiment1Result> summary_rows;
    summary_rows.reserve(DATA_SIZES_MB.size());

    // Run experiment for each data size
    for (size_t data_size_mb : DATA_SIZES_MB) {
        run_experiment_for_size(data_size_mb, csv_writer, rng, summary_rows);
    }

    std::cout << "\n=== Experiment Complete ===" << std::endl;
    std::cout << "Results written to: experiments/results/experiment1_results.csv" << std::endl;

    print_summary_table(summary_rows);

    return 0;
}
