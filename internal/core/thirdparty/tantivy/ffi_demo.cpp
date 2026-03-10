#include <string>
#include <vector>

#include "tantivy-binding.h"

int
main(int argc, char* argv[]) {
    std::vector<std::string> data{"data1", "data2", "data3"};
    std::vector<const char*> data{};
    for (auto& s : data) {
        data.push_back(s.c_str());
    }

    print_vector_of_strings(data.data(), data.size());

    return 0;
}
