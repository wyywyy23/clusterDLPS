#include <vector>
#include <string>
#include "helper.h"

std::vector<std::string> splitString(std::string string_in, std::string delimiter) {
    std::vector<std::string> strings_out;
    size_t pos = 0;
    while ((pos = string_in.find(delimiter)) != std::string::npos) {
        strings_out.push_back(string_in.substr(0, pos));
        string_in.erase(0, pos + delimiter.length());
    }
    strings_out.push_back(string_in);

    return strings_out;
}

