#include "src/core/database.h"
#include <iostream>

int main() {
    std::cout << "Key-Value Database Demo (Integer Only)" << std::endl;
    std::cout << "=====================================" << std::endl;

    Database<int, int> db("test_db", 5);

    if (!db.open()) {
        std::cerr << "Failed to open database!" << std::endl;
        return 1;
    }

    std::cout << "\nInserting key-value pairs into database..." << std::endl;

    db.put(1, 100);
    db.put(2, 200);
    db.put(3, 300);
    db.put(4, 400);
    db.put(5, 500);

    db.print_stats();

    std::cout << "\nRetrieving values:" << std::endl;
    int value;

    if (db.get(1, value)) {
        std::cout << "Key 1: " << value << std::endl;
    }

    if (db.get(2, value)) {
        std::cout << "Key 2: " << value << std::endl;
    }

    if (db.get(3, value)) {
        std::cout << "Key 3: " << value << std::endl;
    }

    if (db.get(4, value)) {
        std::cout << "Key 4: " << value << std::endl;
    }

    if (db.get(5, value)) {
        std::cout << "Key 5: " << value << std::endl;
    }

    std::cout << "\nInserting more data to trigger SST creation:" << std::endl;
    db.put(6, 600);
    db.put(7, 700);
    db.put(8, 800);

    db.print_stats();

    std::cout << "\nTesting retrieval after SST flush:" << std::endl;
    if (db.get(1, value)) {
        std::cout << "Key 1: " << value << std::endl;
    }

    if (db.get(6, value)) {
        std::cout << "Key 6: " << value << std::endl;
    }

    std::cout << "\nTesting scan functionality:" << std::endl;
    size_t result_size;
    std::pair<int, int>* results = db.scan(3, 6, result_size);
    if (results) {
        std::cout << "Scan results (keys 3-6): ";
        for (size_t i = 0; i < result_size; i++) {
            std::cout << "(" << results[i].first << "," << results[i].second << ") ";
        }
        std::cout << std::endl;
        delete[] results;
    }

    if (!db.close()) {
        std::cerr << "Failed to close database!" << std::endl;
        return 1;
    }

    return 0;
}
