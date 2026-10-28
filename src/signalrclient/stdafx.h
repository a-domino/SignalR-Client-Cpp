// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#define STRINGIFY_2(s) #s
#define STRINGIFY(s) STRINGIFY_2(s)
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#ifdef _WIN32 // used in the default log writer and to build the dll

// prevents from defining min/max macros that conflict with std::min()/std::max() functions
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

#endif

#include <functional>
#include <unordered_map>

inline std::string formatException(const std::exception_ptr& ex) noexcept {
    if (!ex) {
        return "None";
    }
    try {
        std::rethrow_exception(ex);
    } catch (const std::exception& e) {
        return e.what();
    } catch (...) {
        return "Unknown non-standard exception";
    }
}

template <typename K, typename V>
void displayMap(std::ostream& os, const std::map<K, V>& m) {
    os << "{";

    bool first = true;
    for (const auto& kvp : m) {
        if (!first) {
            os << ", ";
        }

        os << kvp.first << ": " << kvp.second;
        first = false;
    }

    os << "}";
}


template <typename V>
void displayVector(
    std::ostream& os, const std::vector<V>& v,
    const std::pair<std::string, std::string>& start_end_delimiters = {"[", "]"}
) {
    os << start_end_delimiters.first;

    bool first = true;
    for (const auto& value : v) {
        if (!first) {
            os << ", ";
        }
        os << value;
        first = false;
    }

    os << start_end_delimiters.second;
}
