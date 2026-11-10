// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "signalrclient/signalr_value.h"
#include "signalrclient/signalr_exception.h"

using namespace signalr;

TEST(signalr_value_ext, is_ext_and_type_returns_correct_values)
{
    std::vector<uint8_t> data{0x01, 0x02, 0x03};
    value val(5, data);

    ASSERT_TRUE(val.is_ext());
    ASSERT_FALSE(val.is_null());
    ASSERT_FALSE(val.is_bool());
    ASSERT_FALSE(val.is_double());
    ASSERT_FALSE(val.is_string());
    ASSERT_FALSE(val.is_array());
    ASSERT_FALSE(val.is_map());
    ASSERT_FALSE(val.is_binary());
    ASSERT_EQ(value_type::ext, val.type());
}

TEST(signalr_value_ext, as_ext_returns_correct_type_code_and_payload)
{
    std::vector<uint8_t> data{0xDE, 0xAD, 0xBE, 0xEF};
    value val(-1, data);

    const auto& ext = val.as_ext();
    ASSERT_EQ(-1, ext.type_code);
    ASSERT_EQ(data, ext.data);
}

TEST(signalr_value_ext, can_construct_with_rvalue_payload)
{
    std::vector<uint8_t> data{0xCA, 0xFE};
    value val(42, std::move(data));

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(42, val.as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0xCA, 0xFE}), val.as_ext().data);
}

TEST(signalr_value_ext, can_construct_with_empty_payload)
{
    value val(0, std::vector<uint8_t>{});

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(0, val.as_ext().type_code);
    ASSERT_TRUE(val.as_ext().data.empty());
}

TEST(signalr_value_ext, default_constructor_for_ext_type_creates_empty_ext)
{
    value val(value_type::ext);

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(0, val.as_ext().type_code);
    ASSERT_TRUE(val.as_ext().data.empty());
}

// ReSharper disable CppExpressionWithoutSideEffects
TEST(signalr_value_ext, wrong_as_getters_throw_signalr_exception)
{
    value val(10, std::vector<uint8_t>{0x01});

    try
    {
        val.as_double();
        FAIL() << "expected signalr_exception";
    }
    catch (const signalr_exception& e)
    {
        ASSERT_STREQ("object is a 'ext' expected it to be a 'float64'", e.what());
    }

    try
    {
        val.as_string();
        FAIL() << "expected signalr_exception";
    }
    catch (const signalr_exception& e)
    {
        ASSERT_STREQ("object is a 'ext' expected it to be a 'string'", e.what());
    }

    try
    {
        val.as_bool();
        FAIL() << "expected signalr_exception";
    }
    catch (const signalr_exception& e)
    {
        ASSERT_STREQ("object is a 'ext' expected it to be a 'boolean'", e.what());
    }

    try
    {
        val.as_array();
        FAIL() << "expected signalr_exception";
    }
    catch (const signalr_exception& e)
    {
        ASSERT_STREQ("object is a 'ext' expected it to be an 'array'", e.what());
    }

    try
    {
        val.as_map();
        FAIL() << "expected signalr_exception";
    }
    catch (const signalr_exception& e)
    {
        ASSERT_STREQ("object is a 'ext' expected it to be a 'map'", e.what());
    }

    try
    {
        val.as_binary();
        FAIL() << "expected signalr_exception";
    }
    catch (const signalr_exception& e)
    {
        ASSERT_STREQ("object is a 'ext' expected it to be a 'binary'", e.what());
    }
}
// ReSharper enable CppExpressionWithoutSideEffects

TEST(signalr_value_ext, non_ext_values_throw_on_as_ext)
{
    value num(12.34);
    ASSERT_THROW(num.as_ext(), signalr_exception);

    value str("text");
    ASSERT_THROW(str.as_ext(), signalr_exception);

    value nil;
    ASSERT_THROW(nil.as_ext(), signalr_exception);
}

TEST(signalr_value_ext, copy_construction_copies_ext_deeply)
{
    const std::vector<uint8_t> data{0x10, 0x20};
    const value original(3, data);

    const value& copy(original);

    ASSERT_TRUE(copy.is_ext());
    ASSERT_EQ(3, copy.as_ext().type_code);
    ASSERT_EQ(data, copy.as_ext().data);

    // Verify copy is completely independent
    ASSERT_EQ(original.as_ext().type_code, copy.as_ext().type_code);
    ASSERT_EQ(original.as_ext().data, copy.as_ext().data);
}

TEST(signalr_value_ext, move_construction_moves_ext_data)
{
    std::vector<uint8_t> data{0x10, 0x20, 0x30};
    value original(7, data);

    value moved(std::move(original));

    ASSERT_TRUE(moved.is_ext());
    ASSERT_EQ(7, moved.as_ext().type_code);
    ASSERT_EQ(data, moved.as_ext().data);
}

TEST(signalr_value_ext, copy_and_move_assignment_to_ext)
{
    value source(9, std::vector<uint8_t>{0xAA, 0xBB});
    value target;

    // Assign EXT to non-EXT
    target = source;
    ASSERT_TRUE(target.is_ext());
    ASSERT_EQ(source.as_ext().type_code, target.as_ext().type_code);
    ASSERT_EQ(source.as_ext().data, target.as_ext().data);

    // Assign non-EXT to EXT (verifying proper cleanup of previous ext storage)
    target = value("replacement string");
    ASSERT_TRUE(target.is_string());
    ASSERT_EQ("replacement string", target.as_string());

    // Assign EXT back via move assignment
    target = value(-5, std::vector<uint8_t>{0xCC});
    ASSERT_TRUE(target.is_ext());
    ASSERT_EQ(-5, target.as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0xCC}), target.as_ext().data);
}

TEST(signalr_value_ext, stream_operator_formats_ext)
{
    const value val(2, std::vector<uint8_t>{0x01, 0xFF});
    std::stringstream ss;
    ss << val;
    std::cout << ss.str() << std::endl;

    ASSERT_EQ("{type_code: 2, [1, 255]}", ss.str());
}

TEST(signalr_value_ext_edge_cases, self_assignment_is_safe)
{
    value val(10, std::vector<uint8_t>{0x01, 0x02, 0x03});

    // Self copy-assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign-overload"
    // ReSharper disable once CppIdenticalOperandsInBinaryExpression
    val = val;
#pragma clang diagnostic pop

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(10, val.as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0x01, 0x02, 0x03}), val.as_ext().data);

    // Self move-assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
    val = std::move(val);
#pragma clang diagnostic pop

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(10, val.as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0x01, 0x02, 0x03}), val.as_ext().data);
}

TEST(signalr_value_ext_edge_cases, boundary_type_codes)
{
    // Minimum int8_t
    value min_ext(static_cast<int8_t>(-128), std::vector<uint8_t>{0xAA});
    ASSERT_EQ(-128, min_ext.as_ext().type_code);

    // Reserved MessagePack Timestamp extension (-1)
    value timestamp_ext(static_cast<int8_t>(-1), std::vector<uint8_t>{0x00, 0x00, 0x00, 0x00});
    ASSERT_EQ(-1, timestamp_ext.as_ext().type_code);

    // Maximum int8_t
    value max_ext(static_cast<int8_t>(127), std::vector<uint8_t>{0xBB});
    ASSERT_EQ(127, max_ext.as_ext().type_code);
}

TEST(signalr_value_ext_edge_cases, empty_ext_stream_operator_formatting)
{
    const value empty_ext(-1, std::vector<uint8_t>{});
    std::stringstream ss;
    ss << empty_ext;

    ASSERT_EQ("{type_code: -1, []}", ss.str());
}

