// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "json_helpers.h"
#include <cmath>
#include <cstdint>

namespace signalr {
    signalr::value createValue(const Json::Value& v) {
        switch (v.type()) {
        case Json::ValueType::booleanValue:
            return {v.asBool()};
        case Json::ValueType::realValue:
        case Json::ValueType::intValue:
        case Json::ValueType::uintValue:
            return {v.asDouble()};
        case Json::ValueType::stringValue:
            return {v.asString()};
        case Json::ValueType::arrayValue: {
            std::vector<signalr::value> vec;
            for (const auto& val : v) {
                vec.push_back(createValue(val));
            }
            return {std::move(vec)};
        }
        case Json::ValueType::objectValue: {
            std::map<std::string, signalr::value> map;
            for (const auto& val : v.getMemberNames()) {
                map.insert({val, createValue(v[val])});
            }
            return {std::move(map)};
        }
        case Json::ValueType::nullValue:
        default:
            return {};
        }
    }

    std::string base64Encode(const std::vector<uint8_t>& data) {
        static constexpr char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

        if (data.empty()) {
            return {};
        }

        std::string base64result;
        base64result.reserve(((data.size() + 2) / 3) * 4);

        size_t i = 0;
        while (i + 3 <= data.size()) {
            const uint32_t b = (static_cast<uint32_t>(data[i]) << 16) |
                (static_cast<uint32_t>(data[i + 1]) << 8) |
                static_cast<uint32_t>(data[i + 2]);

            base64result.push_back(table[(b >> 18) & 0x3F]);
            base64result.push_back(table[(b >> 12) & 0x3F]);
            base64result.push_back(table[(b >> 6) & 0x3F]);
            base64result.push_back(table[b & 0x3F]);
            i += 3;
        }

        const size_t remainder = data.size() - i;
        if (remainder == 2) {
            const uint32_t b = (static_cast<uint32_t>(data[i]) << 8) | static_cast<uint32_t>(data[i + 1]);
            base64result.push_back(table[(b >> 10) & 0x3F]);
            base64result.push_back(table[(b >> 4) & 0x3F]);
            base64result.push_back(table[(b << 2) & 0x3F]);
            base64result.push_back('=');
        } else if (remainder == 1) {
            const auto b = static_cast<uint32_t>(data[i]);
            base64result.push_back(table[(b >> 2) & 0x3F]);
            base64result.push_back(table[(b << 4) & 0x3F]);
            base64result.push_back('=');
            base64result.push_back('=');
        }

        return base64result;
    }

    Json::Value createJson(const signalr::value& v) {
        switch (v.type()) {
        case signalr::value_type::boolean:
            return {v.as_bool()};
        case signalr::value_type::float64: {
            const auto value = v.as_double();
            double intPart = 0.0;
            if (std::isfinite(value) && std::modf(value, &intPart) == 0.0) {
                if (value < 0.0) {
                    if (value >= static_cast<double>(INT64_MIN)) {
                        return {static_cast<int64_t>(intPart)};
                    }
                    return {value};
                }
                if (value <= static_cast<double>(UINT64_MAX)) {
                    return {static_cast<uint64_t>(intPart)};
                }
                return {value};
            }
            return {v.as_double()};
        }
        case signalr::value_type::string:
            return {v.as_string()};
        case signalr::value_type::array: {
            const auto& array = v.as_array();
            Json::Value vec(Json::ValueType::arrayValue);
            for (const auto& val : array) {
                vec.append(createJson(val));
            }
            return vec;
        }
        case signalr::value_type::map: {
            const auto& obj = v.as_map();
            Json::Value object(Json::ValueType::objectValue);
            for (const auto& val : obj) {
                object[val.first] = createJson(val.second);
            }
            return object;
        }
        case signalr::value_type::binary: {
            const auto& binary = v.as_binary();
            return {base64Encode(binary)};
        }
        case signalr::value_type::null:
        default:
            return {Json::ValueType::nullValue};
        }
    }

    Json::StreamWriterBuilder getJsonWriter() {
        auto writer = Json::StreamWriterBuilder();
        writer["commentStyle"] = "None";
        writer["indentation"] = "";
        return writer;
    }

    std::unique_ptr<Json::CharReader> getJsonReader() {
        auto builder = Json::CharReaderBuilder();
        Json::CharReaderBuilder::strictMode(&builder.settings_);
        return std::unique_ptr<Json::CharReader>(builder.newCharReader());
    }
}
