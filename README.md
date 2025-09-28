# About

## Directory Structure

-   **`src/`**: Contains all implementation files
-   **`tests/`**: Testing code
-   **`utils/`**: Utility functions and helper code
-   **`data/`**: Runtime directory where database files are stored

## Building

```bash
make all

# Build only main program
make db_main

# Build only tests
make test_memtable

# Clean build artifacts
make clean

# Rebuild everything
make rebuild
```

## Running

```bash
# Run the main demonstration program
make run

# Run the test suite
make test
```

## API

### Database Class

```cpp
Database<K, V> db("database_name", memtable_size);
```

### Core Methods

-   `bool open()` - Open/create database
-   `bool close()` - Close database and flush memtable
-   `bool put(const K& key, const V& value)` - Insert/update key-value pair
-   `bool get(const K& key, V& value)` - Retrieve value by key
-   `std::vector<std::pair<K, V>> scan(const K& start, const K& end)` - Range query
-   `void print_stats()` - Display database statistics

### Status Methods

-   `bool is_database_open() const` - Check if database is open
-   `size_t get_sst_count() const` - Get number of SST files
-   `size_t get_memtable_size() const` - Get current memtable size

## Example Usage

```cpp
#include "include/core/database.h"

// Create database with memtable size of 1000
Database<std::string, std::string> db("my_database", 1000);

// Open database
if (!db.open()) {
    std::cerr << "Failed to open database!" << std::endl;
    return 1;
}

// Insert data
db.put("user:1:name", "Alice");
db.put("user:1:age", "25");
db.put("user:2:name", "Bob");

// Retrieve data
std::string name;
if (db.get("user:1:name", name)) {
    std::cout << "User 1 name: " << name << std::endl;
}

// Range scan (when implemented)
auto results = db.scan("user:1:", "user:1:~");

// Close database
db.close();
```

## Testing

Run tests with:

```bash
make test
```
