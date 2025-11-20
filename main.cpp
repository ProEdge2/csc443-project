#include "src/core/database.h"
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

namespace {
void print_banner() {
    std::cout << "Key-Value Database CLI (Integer Keys/Values)\n"
                 "============================================\n"
                 "Available commands:\n"
                 "  put <key> <value>\n"
                 "  get <key>\n"
                 "  scan <start_key> <end_key>\n"
                 "  delete <key>\n"
                 "  stats\n"
                 "  help\n"
                 "  exit\n"
              << std::endl;
}

void print_help() {
    std::cout << "Commands:\n"
                 "  put <key> <value>      Insert or update an integer key/value pair\n"
                 "  get <key>              Look up a key and print the stored value\n"
                 "  scan <start> <end>     Inclusive range scan; prints ordered key/value pairs\n"
                 "  delete <key>           Insert a tombstone for the key\n"
                 "  stats                  Print internal LSM statistics\n"
                 "  help                   Show this help text\n"
                 "  exit                   Flush state and quit\n"
              << std::endl;
}
} // namespace

int main() {
    Database<int, int> db("interactive_db", 1000);

    if (!db.open()) {
        std::cerr << "Failed to open database!" << std::endl;
        return 1;
    }

    print_banner();

    std::string line;
    while (true) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) {
            std::cout << "\nEnd of input detected, exiting..." << std::endl;
            break;
        }

        std::istringstream iss(line);
        std::string command;
        if (!(iss >> command)) {
            continue;
        }

        if (command == "put") {
            int key;
            int value;
            if (!(iss >> key >> value)) {
                std::cout << "Usage: put <key> <value>" << std::endl;
                continue;
            }
            if (db.put(key, value)) {
                std::cout << "OK" << std::endl;
            } else {
                std::cout << "ERROR: insert failed (memtable full?)" << std::endl;
            }
        } else if (command == "get") {
            int key;
            if (!(iss >> key)) {
                std::cout << "Usage: get <key>" << std::endl;
                continue;
            }
            int value;
            if (db.get(key, value)) {
                std::cout << key << " => " << value << std::endl;
            } else {
                std::cout << "NOT FOUND" << std::endl;
            }
        } else if (command == "scan") {
            int start_key;
            int end_key;
            if (!(iss >> start_key >> end_key)) {
                std::cout << "Usage: scan <start_key> <end_key>" << std::endl;
                continue;
            }
            if (start_key > end_key) {
                std::cout << "ERROR: start_key must be <= end_key" << std::endl;
                continue;
            }
            size_t result_size = 0;
            std::pair<int, int>* results = db.scan(start_key, end_key, result_size);
            if (!results || result_size == 0) {
                std::cout << "[]" << std::endl;
                delete[] results;
                continue;
            }

            std::cout << "[";
            for (size_t i = 0; i < result_size; ++i) {
                std::cout << "(" << results[i].first << ", " << results[i].second << ")";
                if (i + 1 < result_size) {
                    std::cout << ", ";
                }
            }
            std::cout << "]" << std::endl;
            delete[] results;
        } else if (command == "delete") {
            int key;
            if (!(iss >> key)) {
                std::cout << "Usage: delete <key>" << std::endl;
                continue;
            }
            if (db.remove(key)) {
                std::cout << "OK" << std::endl;
            } else {
                std::cout << "ERROR: delete failed" << std::endl;
            }
        } else if (command == "stats") {
            db.print_stats();
        } else if (command == "help") {
            print_help();
        } else if (command == "exit") {
            std::cout << "Shutting down..." << std::endl;
            break;
        } else {
            std::cout << "Unknown command '" << command << "'. Type 'help' for usage." << std::endl;
        }
    }

    if (!db.close()) {
        std::cerr << "Failed to close database cleanly!" << std::endl;
        return 1;
    }

    return 0;
}
