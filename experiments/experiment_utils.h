#ifndef EXPERIMENT_UTILS_H
#define EXPERIMENT_UTILS_H

#include <cmath>
#include <chrono>
#include <fstream>
#include <random>
#include <vector>
#include <string>
#include <iomanip>
#include <filesystem>
#include <unordered_set>
#include <stdexcept>
#include <algorithm>
#include <limits>

// Calculate false positive rate from bits per entry
// Formula: For b bits per entry, FPR = e^(-b * ln(2)^2)
inline double calculate_fpr_from_bits_per_entry(size_t bits_per_entry) {
    return std::exp(-static_cast<double>(bits_per_entry) * std::log(2.0) * std::log(2.0));
}

// Timer utility for measuring operation duration
class Timer {
private:
    std::chrono::high_resolution_clock::time_point start_time;
    std::chrono::high_resolution_clock::time_point end_time;
    bool is_running;

public:
    Timer() : is_running(false) {}

    void start() {
        start_time = std::chrono::high_resolution_clock::now();
        is_running = true;
    }

    void stop() {
        if (is_running) {
            end_time = std::chrono::high_resolution_clock::now();
            is_running = false;
        }
    }

    double elapsed_seconds() const {
        if (is_running) {
            auto now = std::chrono::high_resolution_clock::now();
            return std::chrono::duration<double>(now - start_time).count();
        }
        return std::chrono::duration<double>(end_time - start_time).count();
    }

    double elapsed_milliseconds() const {
        return elapsed_seconds() * 1000.0;
    }
};

// CSV writer utility
class CSVWriter {
private:
    std::ofstream file;
    bool header_written;
    std::vector<std::string> headers;

public:
    CSVWriter(const std::string& filename) : header_written(false) {
        file.open(filename);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open CSV file: " + filename);
        }
    }

    ~CSVWriter() {
        if (file.is_open()) {
            file.close();
        }
    }

    void write_header(const std::vector<std::string>& header_names) {
        if (header_written) {
            return;
        }
        headers = header_names;
        for (size_t i = 0; i < header_names.size(); ++i) {
            file << header_names[i];
            if (i < header_names.size() - 1) {
                file << ",";
            }
        }
        file << "\n";
        header_written = true;
    }

    void write_row(const std::vector<std::string>& values) {
        if (!header_written) {
            throw std::runtime_error("Header must be written before rows");
        }
        for (size_t i = 0; i < values.size(); ++i) {
            file << values[i];
            if (i < values.size() - 1) {
                file << ",";
            }
        }
        file << "\n";
    }

    template<typename T>
    void write_row(const std::vector<T>& values) {
        std::vector<std::string> str_values;
        for (const auto& val : values) {
            str_values.push_back(std::to_string(val));
        }
        write_row(str_values);
    }
};

// Random number generator utility
class RandomGenerator {
private:
    std::mt19937 gen;
    std::uniform_int_distribution<int> int_dist;

public:
    RandomGenerator(unsigned int seed = std::random_device{}()) : gen(seed), int_dist(0, std::numeric_limits<int>::max()) {}

    // Generate random integer in range [min, max]
    int random_int(int min, int max) {
        std::uniform_int_distribution<int> dist(min, max);
        return dist(gen);
    }

    // Generate random integer (uses default distribution)
    int random_int() {
        return int_dist(gen);
    }

    // Generate vector of unique random integers
    std::vector<int> generate_unique_random_ints(size_t count, int min, int max) {
        if (static_cast<size_t>(max - min + 1) < count) {
            throw std::runtime_error("Range too small for unique values");
        }

        std::vector<int> result;
        std::unordered_set<int> seen;
        std::uniform_int_distribution<int> dist(min, max);

        while (result.size() < count) {
            int val = dist(gen);
            if (seen.find(val) == seen.end()) {
                seen.insert(val);
                result.push_back(val);
            }
        }

        return result;
    }

    // Generate vector of random integers (may have duplicates)
    std::vector<int> generate_random_ints(size_t count, int min, int max) {
        std::vector<int> result;
        result.reserve(count);
        std::uniform_int_distribution<int> dist(min, max);

        for (size_t i = 0; i < count; ++i) {
            result.push_back(dist(gen));
        }

        return result;
    }

    // Shuffle a vector
    template<typename T>
    void shuffle(std::vector<T>& vec) {
        std::shuffle(vec.begin(), vec.end(), gen);
    }
};

// Calculate throughput (operations per second)
inline double calculate_throughput(size_t operations, double elapsed_seconds) {
    if (elapsed_seconds <= 0.0) {
        return 0.0;
    }
    return static_cast<double>(operations) / elapsed_seconds;
}

// Ensure directory exists
inline void ensure_directory_exists(const std::string& dir_path) {
    std::filesystem::path path(dir_path);
    if (!std::filesystem::exists(path)) {
        std::filesystem::create_directories(path);
    }
}

#endif // EXPERIMENT_UTILS_H
