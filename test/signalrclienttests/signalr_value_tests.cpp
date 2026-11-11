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
    value val(5, data.data(), data.size(), std::make_shared<std::vector<uint8_t>>(std::move(data)));

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
    const auto data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0xDE, 0xAD, 0xBE, 0xEF});
    const value val(-1, data->data(), data->size(), data);

    const auto& ext = val.as_ext();
    ASSERT_EQ(-1, ext.type_code);
    ASSERT_EQ(data->size(), ext.data->size);
    for (int i = 0; i < ext.data->size; ++i) {
        ASSERT_EQ((*data)[i], ext.data->buff[i]);
    }
}

TEST(signalr_value_ext, can_construct_with_rvalue_payload)
{
    const auto data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0xCA, 0xFE});
    const value val(42, data->data(), data->size(), data);

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(42, val.as_ext().type_code);
    ASSERT_EQ(2, val.as_ext().data->size);
    ASSERT_EQ(0xCA, val.as_ext().data->buff[0]);
    ASSERT_EQ(0xFE, val.as_ext().data->buff[1]);
}

TEST(signalr_value_ext, can_construct_with_empty_payload)
{
    constexpr int8_t type_code = 2;
    const value val(type_code);

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(type_code, val.as_ext().type_code);
    ASSERT_TRUE(val.as_ext().data == nullptr);
}

TEST(signalr_value_ext, default_constructor_for_ext_type_creates_empty_ext)
{
    value val(value_type::ext);

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(0, val.as_ext().type_code);
    ASSERT_TRUE(val.as_ext().data == nullptr);
}

// ReSharper disable CppExpressionWithoutSideEffects
TEST(signalr_value_ext, wrong_as_getters_throw_signalr_exception)
{
    auto data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0x01});
    const value val(10, data->data(), data->size(), data);

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
    const auto data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0x10, 0x20});
    const value original(3, data->data(), data->size(), data);

    const value& copy(original);

    ASSERT_TRUE(copy.is_ext());
    ASSERT_EQ(3, copy.as_ext().type_code);
    ASSERT_EQ(data, copy.as_ext().data->underlying_buffer);

    // Verify copy is completely independent
    ASSERT_EQ(original.as_ext().type_code, copy.as_ext().type_code);
    ASSERT_EQ(original.as_ext().data->size, copy.as_ext().data->size);
    ASSERT_TRUE(original.as_ext().data == copy.as_ext().data); // underlying data uint8_t* should be the same.
    ASSERT_TRUE(original.as_ext().data->underlying_buffer == copy.as_ext().data->underlying_buffer);
}

TEST(signalr_value_ext, move_construction_moves_ext_data)
{
    const auto data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0x10, 0x20, 0x30});
    value original(7, data->data(), data->size(), data);

    value moved(std::move(original));

    ASSERT_TRUE(moved.is_ext());
    ASSERT_EQ(7, moved.as_ext().type_code);
    ASSERT_EQ(data->data(), moved.as_ext().data->buff);
    ASSERT_EQ(data->size(), moved.as_ext().data->size);
    ASSERT_EQ(data, moved.as_ext().data->underlying_buffer);
}

TEST(signalr_value_ext, copy_and_move_assignment_to_ext)
{
    const auto source_data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0xAA, 0xBB});
    value source(9, source_data->data(), source_data->size(), source_data);
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
    auto target_data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0xCC});
    target = value(-5, target_data->data(), target_data->size(), target_data);
    ASSERT_TRUE(target.is_ext());
    ASSERT_EQ(-5, target.as_ext().type_code);
    ASSERT_EQ(target_data, target.as_ext().data->underlying_buffer);
    ASSERT_EQ((std::vector<uint8_t>{0xCC}),
              *static_cast<const std::vector<uint8_t>*>(target.as_ext().data->underlying_buffer.get()));
    ASSERT_EQ(target_data->data(), target.as_ext().data->buff);
    ASSERT_EQ(1, target.as_ext().data->size);
}

TEST(signalr_value_ext, stream_operator_formats_ext)
{
    const auto data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0x01, 0xFF});
    const value val(2, data->data(), data->size(), data);
    std::stringstream ss;
    ss << val;
    std::cout << ss.str() << std::endl;

    ASSERT_EQ("{type_code: 2, [1, 255]}", ss.str());
}

TEST(signalr_value_ext_edge_cases, self_assignment_is_safe)
{
    auto data_ptr = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{1, 2, 3});
    value val(10, data_ptr->data(), data_ptr->size(), data_ptr);

    // Self copy-assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-assign-overload"
    // ReSharper disable once CppIdenticalOperandsInBinaryExpression
    val = val;
#pragma clang diagnostic pop

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(10, val.as_ext().type_code);
    ASSERT_NE(nullptr, val.as_ext().data);
    ASSERT_EQ(3U, val.as_ext().data->size);
    ASSERT_EQ(data_ptr, val.as_ext().data->underlying_buffer);
    ASSERT_EQ(data_ptr->data(), val.as_ext().data->buff);
    ASSERT_EQ(data_ptr->size(), val.as_ext().data->size);

    // Self move-assignment
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wself-move"
    val = std::move(val);
#pragma clang diagnostic pop

    ASSERT_TRUE(val.is_ext());
    ASSERT_EQ(10, val.as_ext().type_code);
    ASSERT_NE(nullptr, val.as_ext().data);
    ASSERT_EQ(3U, val.as_ext().data->size);
    ASSERT_EQ(data_ptr, val.as_ext().data->underlying_buffer);
    ASSERT_EQ(data_ptr->data(), val.as_ext().data->buff);
    ASSERT_EQ(data_ptr->size(), val.as_ext().data->size);
}

TEST(signalr_value_ext_edge_cases, boundary_type_codes)
{
    // Minimum int8_t
    constexpr int8_t min_type_code = -128;
    auto min_ext_data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0xAA});
    value min_ext(min_type_code, min_ext_data->data(), min_ext_data->size(), min_ext_data);
    ASSERT_EQ(-128, min_ext.as_ext().type_code);

    // Reserved MessagePack Timestamp extension (-1)
    constexpr int8_t reserved_msgpack_timestamp_ext_type_code = -1;
    auto timestamp_data = std::make_shared<std::string>("\0\0\0\0");
    value timestamp_ext(
        reserved_msgpack_timestamp_ext_type_code, reinterpret_cast<const uint8_t*>(timestamp_data->data()),
        timestamp_data->size(), timestamp_data
    );
    ASSERT_EQ(reserved_msgpack_timestamp_ext_type_code, timestamp_ext.as_ext().type_code);

    // Maximum int8_t
    auto max_ext_data = std::make_shared<std::vector<uint8_t>>(std::vector<uint8_t>{0xBB});
    value max_ext(static_cast<int8_t>(127), max_ext_data->data(), max_ext_data->size(), max_ext_data);
    ASSERT_EQ(127, max_ext.as_ext().type_code);
}

TEST(signalr_value_ext_edge_cases, empty_ext_stream_operator_formatting)
{
    const value empty_ext(static_cast<int8_t>(-1));
    std::stringstream ss;
    ss << empty_ext;

    ASSERT_EQ("{type_code: -1, []}", ss.str());
}

TEST(signalr_value_ext_edge_cases, empty_ext_comparison_and_safe_null_data)
{
    // Test that an empty EXT constructed with nullptr data compares equal to another empty EXT
    constexpr int8_t type_code = 5;
    value ext1(type_code);
    value ext2(5, nullptr, 0, nullptr);

    ASSERT_TRUE(ext1.is_ext());
    ASSERT_TRUE(ext2.is_ext());
    EXPECT_EQ(ext1.as_ext(), ext2.as_ext());

    // Different type codes must not match
    constexpr int8_t diff_type_code = 6;
    value ext3(diff_type_code);
    EXPECT_NE(ext1.as_ext(), ext3.as_ext());
}
