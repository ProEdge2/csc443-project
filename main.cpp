#include "src/core/database.h"
#include <iostream>
#include <string>

int main() {
    std::cout << "Key-Value Database Demo" << std::endl;
    std::cout << "=======================" << std::endl;

    Database<std::string, std::string> db("test_db", 5);

    if (!db.open()) {
        std::cerr << "Failed to open database!" << std::endl;
        return 1;
    }

    std::cout << "\nInserting key-value pairs into database..." << std::endl;

    db.put("name", "Alice");
    db.put("age", "25");
    db.put("city", "New York");
    db.put("job", "Engineer");
    db.put("hobby", "Reading");

    db.print_stats();

    std::cout << "\nRetrieving values:" << std::endl;
    std::string value;

    if (db.get("name", value)) {
        std::cout << "name: " << value << std::endl;
    }

    if (db.get("age", value)) {
        std::cout << "age: " << value << std::endl;
    }

    if (db.get("city", value)) {
        std::cout << "city: " << value << std::endl;
    }

    if (db.get("job", value)) {
        std::cout << "job: " << value << std::endl;
    }

    if (db.get("hobby", value)) {
        std::cout << "hobby: " << value << std::endl;
    }

    std::cout << "\nInserting more data to trigger SST creation:" << std::endl;
    db.put("country", "USA");
    db.put("state", "California");
    db.put("company", "TechCorp");

    db.print_stats();

    std::cout << "\nTesting retrieval after SST flush:" << std::endl;
    if (db.get("name", value)) {
        std::cout << "name: " << value << std::endl;
    }

    if (db.get("country", value)) {
        std::cout << "country: " << value << std::endl;
    }

    std::cout << "\nDemo with integer database:" << std::endl;
    Database<int, int> int_db("int_test_db", 10);

    if (int_db.open()) {
        for (int i = 1; i <= 7; i++) {
            int_db.put(i, i * i);
        }

        int int_value;
        std::cout << "Square of 5: ";
        if (int_db.get(5, int_value)) {
            std::cout << int_value << std::endl;
        }

        int_db.print_stats();
        int_db.close();
    }

    if (!db.close()) {
        std::cerr << "Failed to close database!" << std::endl;
        return 1;
    }

    return 0;
}
