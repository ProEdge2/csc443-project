#include "../src/core/database.h"
#include "experiment_utils.h"
#include <iostream>
#include <filesystem>
#include <vector>
#include <algorithm>
#include <iomanip>

constexpr size_t BUFFER_POOL_SIZE_MB = 10;
constexpr size_t BUFFER_POOL_PAGES = (BUFFER_POOL_SIZE_MB * 1024 * 1024) / 4096;
constexpr size_t MEMTABLE_SIZE_ENTRIES = (1 * 1024 * 1024) / sizeof(std::pair<int, int>);
constexpr size_t BITS_PER_ENTRY = 8;
const double BLOOM_FILTER_FPR = calculate_fpr_from_bits_per_entry(BITS_PER_ENTRY);

constexpr size_t TOTAL_DATA_SIZE_MB = 1000; // 1 GB (1000MB for consistent 100MB batches)
constexpr size_t MEASUREMENT_INTERVAL_MB = 100; // Measure every 100 MB
constexpr size_t QUERY_BATCH_SIZE = 10000; // Number of queries per measurement
constexpr size_t SCAN_RANGE_SIZE = 1000; // Range size for scan queries

struct Experiment2Result {
    size_t data_size_mb;
    double insert_throughput;
    double get_throughput;
    double scan_throughput;
};

size_t calculate_entry_count_for_size_mb(size_t size_mb) {
    // Each entry is a pair<int, int> = 8 bytes
    return (size_mb * 1024 * 1024) / sizeof(std::pair<int, int>);
}

void cleanup_database(const std::string& db_name) {
    std::string db_dir = "data/" + db_name;
    if (std::filesystem::exists(db_dir)) {
        std::filesystem::remove_all(db_dir);
    }
}

double measure_insert_throughput(Database<int, int>& db, const std::vector<int>& keys_to_insert, size_t num_inserts) {
    Timer insert_timer;
    insert_timer.start();

    for (size_t i = 0; i < num_inserts && i < keys_to_insert.size(); ++i) {
        db.put(keys_to_insert[i], keys_to_insert[i] * 10);
    }

    insert_timer.stop();
    return calculate_throughput(num_inserts, insert_timer.elapsed_seconds());
}

double measure_get_throughput(Database<int, int>& db, const std::vector<int>& query_keys, size_t num_queries) {
    Timer query_timer;
    query_timer.start();

    int value;
    for (size_t i = 0; i < num_queries; ++i) {
        db.get(query_keys[i % query_keys.size()], value);
    }

    query_timer.stop();
    return calculate_throughput(num_queries, query_timer.elapsed_seconds());
}

double measure_scan_throughput(Database<int, int>& db, const std::vector<std::pair<int, int>>& scan_ranges, size_t num_scans) {
    Timer scan_timer;
    scan_timer.start();

    size_t result_size;
    for (size_t i = 0; i < num_scans; ++i) {
        const auto& range = scan_ranges[i % scan_ranges.size()];
        auto* results = db.scan(range.first, range.second, result_size);
        if (results) {
            delete[] results;
        }
    }

    scan_timer.stop();
    return calculate_throughput(num_scans, scan_timer.elapsed_seconds());
}

void measure_throughput_at_interval(Database<int, int>& db, size_t current_size_mb,
                                    RandomGenerator& rng, CSVWriter& csv_writer,
                                    std::vector<int>& inserted_keys, double insert_throughput,
                                    std::vector<Experiment2Result>& summary_rows) {
    std::cout << "\n=== Measuring at " << current_size_mb << " MB ===" << std::endl;

    double get_tp = 0.0, scan_tp = 0.0;

    // Use the actual insertion throughput from the main insertion loop
    double insert_tp = insert_throughput;
    std::cout << "Insert throughput: " << insert_tp << " inserts/sec (from main insertion)" << std::endl;

    // Measure get throughput
    std::cout << "Measuring get throughput..." << std::flush;
    if (!inserted_keys.empty()) {
        // Generate random query keys from inserted keys
        std::vector<int> query_keys;
        query_keys.reserve(QUERY_BATCH_SIZE);
        for (size_t i = 0; i < QUERY_BATCH_SIZE; ++i) {
            query_keys.push_back(inserted_keys[rng.random_int(0, inserted_keys.size() - 1)]);
        }
        get_tp = measure_get_throughput(db, query_keys, QUERY_BATCH_SIZE);
        std::cout << " " << get_tp << " gets/sec" << std::endl;
    } else {
        std::cout << " N/A (no data yet)" << std::endl;
    }

    // Measure scan throughput
    std::cout << "Measuring scan throughput..." << std::flush;
    if (!inserted_keys.empty()) {
        // Generate random scan ranges
        std::vector<std::pair<int, int>> scan_ranges;
        scan_ranges.reserve(QUERY_BATCH_SIZE);
        for (size_t i = 0; i < QUERY_BATCH_SIZE; ++i) {
            int start_key = inserted_keys[rng.random_int(0, inserted_keys.size() - 1)];
            int end_key = start_key + SCAN_RANGE_SIZE;
            scan_ranges.push_back({start_key, end_key});
        }
        scan_tp = measure_scan_throughput(db, scan_ranges, QUERY_BATCH_SIZE);
        std::cout << " " << scan_tp << " scans/sec" << std::endl;
    } else {
        std::cout << " N/A (no data yet)" << std::endl;
    }

    // Write results to CSV
    csv_writer.write_row({std::to_string(current_size_mb),
                          std::to_string(insert_tp),
                          std::to_string(get_tp),
                          std::to_string(scan_tp)});
    summary_rows.push_back({current_size_mb, insert_tp, get_tp, scan_tp});
}

int main() {
    std::cout << "=== Experiment 2: Throughput Over Time as Data Grows ===" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  Buffer pool: " << BUFFER_POOL_SIZE_MB << " MB (" << BUFFER_POOL_PAGES << " pages)" << std::endl;
    std::cout << "  Memtable: 1 MB (" << MEMTABLE_SIZE_ENTRIES << " entries)" << std::endl;
    std::cout << "  Bloom filter: " << BITS_PER_ENTRY << " bits per entry (FPR ≈ " << BLOOM_FILTER_FPR << ")" << std::endl;
    std::cout << "  Total data size: " << TOTAL_DATA_SIZE_MB << " MB" << std::endl;
    std::cout << "  Measurement interval: " << MEASUREMENT_INTERVAL_MB << " MB" << std::endl;
    std::cout << "  Query batch size: " << QUERY_BATCH_SIZE << " queries" << std::endl;
    std::cout << std::endl;

    // Ensure results directory exists
    ensure_directory_exists("experiments/results");

    // Clean up any existing database
    std::string db_name = "exp2_throughput";
    cleanup_database(db_name);

    // Create and open database
    Database<int, int> db(db_name, MEMTABLE_SIZE_ENTRIES, BLOOM_FILTER_FPR, BUFFER_POOL_PAGES);
    if (!db.open()) {
        std::cerr << "Failed to open database!" << std::endl;
        return 1;
    }

    // Create CSV writer
    CSVWriter csv_writer("experiments/results/experiment2_results.csv");
    csv_writer.write_header({"data_size_mb", "insert_throughput", "get_throughput", "scan_throughput"});

    // Initialize random generator with fixed seed for reproducibility
    RandomGenerator rng(83);

    // Track inserted keys for query generation
    std::vector<int> inserted_keys;
    inserted_keys.reserve(calculate_entry_count_for_size_mb(TOTAL_DATA_SIZE_MB));

    std::vector<Experiment2Result> summary_rows;
    summary_rows.reserve(TOTAL_DATA_SIZE_MB / MEASUREMENT_INTERVAL_MB + 2);

    // Insert data and measure at intervals
    size_t total_entries = calculate_entry_count_for_size_mb(TOTAL_DATA_SIZE_MB);
    size_t entries_per_interval = calculate_entry_count_for_size_mb(MEASUREMENT_INTERVAL_MB);
    size_t current_entries = 0;
    size_t next_measurement_mb = MEASUREMENT_INTERVAL_MB;

    std::cout << "Inserting " << total_entries << " entries total..." << std::endl;

    int max_key = std::numeric_limits<int>::max() / 2;

    double last_insert_throughput = 0.0;

    while (current_entries < total_entries) {
        // Insert entries for this interval
        size_t entries_to_insert = std::min(entries_per_interval, total_entries - current_entries);
        std::cout << "\nInserting " << entries_to_insert << " entries (total: " << current_entries << "/" << total_entries << ")..." << std::flush;

        std::vector<int> new_keys = rng.generate_unique_random_ints(entries_to_insert, 0, max_key);

        Timer insert_timer;
        insert_timer.start();
        for (int key : new_keys) {
            db.put(key, key * 10);
        }
        insert_timer.stop();

        // Calculate actual insertion throughput for this batch
        last_insert_throughput = calculate_throughput(entries_to_insert, insert_timer.elapsed_seconds());

        inserted_keys.insert(inserted_keys.end(), new_keys.begin(), new_keys.end());
        current_entries += entries_to_insert;

        std::cout << " Done (" << insert_timer.elapsed_seconds() << "s, " << last_insert_throughput << " inserts/sec)" << std::endl;

        // Measure throughput at this interval
        size_t current_size_mb = (current_entries * sizeof(std::pair<int, int>)) / (1024 * 1024);
        if (current_size_mb >= next_measurement_mb || current_entries >= total_entries) {
            measure_throughput_at_interval(db, current_size_mb, rng, csv_writer, inserted_keys, last_insert_throughput, summary_rows);
            next_measurement_mb += MEASUREMENT_INTERVAL_MB;
        }
    }

    // Final measurement (use last measured insert throughput)
    bool already_recorded_final = !summary_rows.empty() &&
                                  summary_rows.back().data_size_mb == TOTAL_DATA_SIZE_MB;
    if (current_entries >= total_entries && last_insert_throughput > 0.0 && !already_recorded_final) {
        measure_throughput_at_interval(db, TOTAL_DATA_SIZE_MB, rng, csv_writer, inserted_keys, last_insert_throughput, summary_rows);
    }

    // Close database
    db.close();

    std::cout << "\n=== Experiment Complete ===" << std::endl;
    std::cout << "Results written to: experiments/results/experiment2_results.csv" << std::endl;

    if (!summary_rows.empty()) {
        std::cout << "\nFinal throughput table (ops/sec):" << std::endl;
        std::cout << std::left << std::setw(12) << "Data MB"
                  << std::right << std::setw(18) << "Insert"
                  << std::right << std::setw(18) << "Get"
                  << std::right << std::setw(18) << "Scan" << std::endl;
        std::cout << std::string(66, '-') << std::endl;
        std::cout << std::fixed << std::setprecision(0);
        for (const auto& row : summary_rows) {
            std::cout << std::left << std::setw(12) << row.data_size_mb
                      << std::right << std::setw(18) << row.insert_throughput
                      << std::right << std::setw(18) << row.get_throughput
                      << std::right << std::setw(18) << row.scan_throughput << std::endl;
        }
        std::cout.unsetf(std::ios::floatfield);
    }

    std::cout << "\nNote: Sequential flooding protection is enabled in the buffer pool." << std::endl;

    return 0;
}
