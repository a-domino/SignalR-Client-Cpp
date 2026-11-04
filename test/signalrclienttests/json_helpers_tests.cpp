// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "json_helpers.h"

using namespace signalr;

TEST(base64_encode, empty_input_returns_empty_string_without_underflow) {
    std::vector<uint8_t> empty_data;
    ASSERT_EQ("", base64Encode(empty_data));
}

TEST(base64_encode, one_byte_encodes_with_two_padding_equals) {
    std::vector<uint8_t> data = {'M'};
    ASSERT_EQ("TQ==", base64Encode(data));
}

TEST(base64_encode, two_bytes_encode_with_one_padding_equal) {
    std::vector<uint8_t> data = {'M', 'a'};
    ASSERT_EQ("TWE=", base64Encode(data));
}

TEST(base64_encode, three_bytes_encode_without_padding) {
    std::vector<uint8_t> data = {'M', 'a', 'n'};
    ASSERT_EQ("TWFu", base64Encode(data));
}

TEST(base64_encode, four_bytes_encodes_correctly) {
    std::vector<uint8_t> data = {'p', 'l', 'e', 'a'};
    ASSERT_EQ("cGxlYQ==", base64Encode(data));
}

TEST(create_json, whole_floating_point_numbers_are_serialized_as_integers) {
    signalr::value float_int(1.0);
    auto json = createJson(float_int);

    ASSERT_TRUE(json.isInt64() || json.isUInt64());
    ASSERT_EQ(1, json.asInt64());
}

TEST(create_json, fractional_floating_point_numbers_remain_double) {
    signalr::value frac(1.25);
    auto json = createJson(frac);

    ASSERT_TRUE(json.isDouble());
    ASSERT_DOUBLE_EQ(1.25, json.asDouble());
}
