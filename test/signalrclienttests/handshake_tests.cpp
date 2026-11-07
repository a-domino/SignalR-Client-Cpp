// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "handshake_protocol.h"
#include "signalrclient/signalr_exception.h"
#include "json_hub_protocol.h"

using namespace signalr;

TEST(handshake, parses_empty_handshake)
{
    std::string remaining;
    signalr::value handshake;
    std::tie(remaining, handshake) = handshake::parse_handshake("{}\x1e");

    ASSERT_STREQ("", remaining.c_str());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_EQ(0, handshake.as_map().size());
}

TEST(handshake, parses_handshake_with_error)
{
    std::string remaining;
    signalr::value handshake;
    std::tie(remaining, handshake) = handshake::parse_handshake("{\"error\":\"something bad\"}\x1e");

    ASSERT_STREQ("", remaining.c_str());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_STREQ("something bad", handshake.as_map().at("error").as_string().c_str());
}

TEST(handshake, throws_for_incomplete_message)
{
    try
    {
        std::string remaining;
        signalr::value handshake;
        std::tie(remaining, handshake) = handshake::parse_handshake("{}");
        ASSERT_TRUE(false);
    }
    catch (const signalr_exception& ex)
    {
        ASSERT_STREQ("incomplete message received", ex.what());
    }
}

TEST(handshake, returns_remaining_data)
{
    std::string remaining;
    signalr::value handshake;
    std::tie(remaining, handshake) = handshake::parse_handshake("{}\x1e{\"type\":6}\x1e");

    ASSERT_STREQ("{\"type\":6}\x1e", remaining.c_str());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_EQ(0, handshake.as_map().size());
}

TEST(handshake, extra_fields_are_fine)
{
    std::string remaining;
    signalr::value handshake;
    std::tie(remaining, handshake) = handshake::parse_handshake("{\"extra\":\"blah\"}\x1e");

    ASSERT_STREQ("", remaining.c_str());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_EQ(1, handshake.as_map().size());
}

TEST(handshake, writes_protocol_and_version)
{
    auto protocol = std::unique_ptr<hub_protocol>(new json_hub_protocol());
    auto message = handshake::write_handshake(protocol);

    ASSERT_STREQ("{\"protocol\":\"json\",\"version\":1}\x1e", message.c_str());
}

namespace {
// Mock protocol for isolating serialization logic
class mock_hub_protocol : public hub_protocol
{
public:
    mock_hub_protocol(
        std::string name,
        const int version,
        const signalr::transfer_format format = signalr::transfer_format::text
    ) : m_name(std::move(name)),
        m_version(version),
        m_format(format) {
    }

    std::string write_invocation_message(
        const invocation_message*
    ) const override {
        return {};
    }

    std::string write_stream_item_message(
        const stream_item_message*
    ) const override {
        return {};
    }

    std::string write_completion_message(
        const completion_message*
    ) const override {
        return {};
    }

    std::string write_stream_invocation_message(
        const stream_invocation_message*
    ) const override {
        return {};
    }

    std::string write_cancel_invocation_message(
        const cancel_invocation_message*
    ) const override {
        return {};
    }

    std::string write_ping_message(
        const ping_message*
    ) const override {
        return {};
    }

    std::string write_close_message(
        const close_message*
    ) const override {
        return {};
    }

    std::string write_ack_message(
        const ack_message*
    ) const override {
        return {};
    }

    std::string write_sequence_message(
        const sequence_message*
    ) const override {
        return {};
    }

    std::unique_ptr<parser_and_handler> create_parser_and_handler(
        const std::shared_ptr<message_handler>&
    ) const override {
        return nullptr;
    }

    const std::string& name() const override {
        return m_name;
    }

    int version() const override {
        return m_version;
    }

    signalr::transfer_format transfer_format() const override {
        return m_format;
    }

private:
    std::string m_name;
    int m_version;
    signalr::transfer_format m_format;
};
}

// -----------------------------------------------------------------------------
// write_handshake Tests
// -----------------------------------------------------------------------------

TEST(handshake_write, serializes_json_hub_protocol) {
    const auto protocol = std::unique_ptr<hub_protocol>(new json_hub_protocol());
    const auto message = handshake::write_handshake(protocol);

    ASSERT_STREQ("{\"protocol\":\"json\",\"version\":1}\x1e", message.c_str());
}

TEST(handshake_write, serializes_custom_and_binary_protocol_names) {
    mock_hub_protocol messagepack_proto(
        "messagepack",
        2,
        signalr::transfer_format::binary
    );
    const auto message = handshake::write_handshake(messagepack_proto);

    ASSERT_STREQ("{\"protocol\":\"messagepack\",\"version\":2}\x1e", message.c_str());
}

TEST(handshake_write, throws_when_unique_ptr_is_null) {
    std::unique_ptr<hub_protocol> null_proto;
    EXPECT_THROW(
        {
            handshake::write_handshake(null_proto);
        },
        std::invalid_argument
    );
}

// -----------------------------------------------------------------------------
// parse_handshake Happy Path Tests
// -----------------------------------------------------------------------------

TEST(handshake_parse, parses_empty_handshake) {
    auto parsed_handshake = handshake::parse_handshake("{}\x1e");
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_TRUE(remaining.empty());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_EQ(0, handshake.as_map().size());
}

TEST(handshake_parse, parses_handshake_with_error) {
    auto parsed_handshake = handshake::parse_handshake("{\"error\":\"server rejected protocol\"}\x1e");
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_TRUE(remaining.empty());
    ASSERT_TRUE(handshake.is_map());
    const auto& map = handshake.as_map();
    ASSERT_NE(map.find("error"), map.end());
    ASSERT_STREQ("server rejected protocol", map.at("error").as_string().c_str());
}

TEST(handshake_parse, parses_handshake_with_minor_version) {
    auto parsed_response = handshake::parse_handshake_message("{\"minorVersion\":1}\x1e");
    const std::string remaining = std::get<0>(parsed_response);
    const handshake::handshake_response response = std::get<1>(parsed_response);

    ASSERT_TRUE(remaining.empty());
    ASSERT_FALSE(response.has_error());
    ASSERT_EQ(1, response.minor_version);
}

TEST(handshake_parse, ignores_unknown_and_extra_fields) {
    auto parsed_handshake = handshake::parse_handshake(
        "{\"unknownField\":true,\"nested\":{\"a\":123}}\x1e"
    );
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_TRUE(remaining.empty());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_EQ(2, handshake.as_map().size());
}

TEST(handshake_parse, handles_leading_and_trailing_whitespace) {
    auto parsed_handshake = handshake::parse_handshake("  \t\r\n{ \"error\" : \"failed\" }  \x1e");
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_TRUE(remaining.empty());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_STREQ("failed", handshake.as_map().at("error").as_string().c_str());
}

TEST(handshake_parse, handles_escaped_characters_and_utf8) {
    auto parsed_handshake = handshake::parse_handshake(
        "{\"error\":\"Line 1\\nLine 2\\t\\\"Quoted\\\" \\u00A9\"}\x1e"
    );
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_TRUE(remaining.empty());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_STREQ("Line 1\nLine 2\t\"Quoted\" \xC2\xA9", handshake.as_map().at("error").as_string().c_str());
}

// -----------------------------------------------------------------------------
// Buffer Splitting & Boundary Tests
// -----------------------------------------------------------------------------

TEST(handshake_parse, extracts_remaining_data_after_separator) {
    auto parsed_handshake = handshake::parse_handshake("{}\x1e{\"type\":6}\x1e");
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_STREQ("{\"type\":6}\x1e", remaining.c_str());
    ASSERT_TRUE(handshake.is_map());
    ASSERT_EQ(0, handshake.as_map().size());
}

TEST(handshake_parse, handles_binary_payload_as_remaining_data) {
    const std::string binary_hub_msg = "\x01\x02\x03\x04";
    const std::string payload = std::string("{}\x1e") + binary_hub_msg;

    auto parsed_handshake = handshake::parse_handshake(payload);
    const auto remaining = std::get<0>(parsed_handshake);
    const auto handshake = std::get<1>(parsed_handshake);

    ASSERT_EQ(binary_hub_msg, remaining);
    ASSERT_TRUE(handshake.is_map());
}

TEST(handshake_parse, preserves_multiple_following_messages) {
    const std::string payload = "{}\x1e{\"type\":6}\x1e{\"type\":1,\"target\":\"Receive\"}\x1e";
    auto parsed_response = handshake::parse_handshake(payload);
    const std::string remaining = std::get<0>(parsed_response);

    ASSERT_STREQ("{\"type\":6}\x1e{\"type\":1,\"target\":\"Receive\"}\x1e", remaining.c_str());
}

// -----------------------------------------------------------------------------
// Error Handling & Negative Tests
// -----------------------------------------------------------------------------

TEST(handshake_parse, throws_for_missing_separator) {
    try {
        handshake::parse_handshake("{}");
        FAIL() << "Expected signalr_exception was not thrown.";
    } catch (const signalr_exception& ex) {
        ASSERT_STREQ("incomplete message received", ex.what());
    }
}

TEST(handshake_parse, throws_for_empty_buffer) {
    try {
        handshake::parse_handshake("");
        FAIL() << "Expected signalr_exception was not thrown.";
    } catch (const signalr_exception& ex) {
        ASSERT_STREQ("incomplete message received", ex.what());
    }
}

TEST(handshake_parse, throws_for_empty_json_before_separator) {
    EXPECT_THROW(
        {
            handshake::parse_handshake("\x1e");
        },
        signalr_exception
    );
}

TEST(handshake_parse, throws_for_malformed_json_syntax) {
    EXPECT_THROW(
        {
            handshake::parse_handshake("{ invalid json }\x1e");
        },
        signalr_exception
    );

    EXPECT_THROW(
        {
            handshake::parse_handshake("{\"error\": \x1e");
        },
        signalr_exception
    );
}

TEST(handshake_parse, throws_when_json_root_is_array) {
    // Arrays parse cleanly under RFC 4627 strict mode, so they are caught by our root.isObject() check
    try {
        handshake::parse_handshake("[1, 2, 3]\x1e");
        FAIL() << "Expected non-object JSON array to throw.";
    } catch (const signalr_exception& ex) {
        ASSERT_STREQ("handshake response must be a JSON object", ex.what());
    }
}

TEST(handshake_parse, throws_when_json_root_is_primitive) {
    // Primitives fail JsonCpp's strictMode (strictRoot = true) parser check before object checks are reached
    EXPECT_THROW(
        {
            handshake::parse_handshake("\"unexpected response\"\x1e");
        },
        signalr_exception
    );

    EXPECT_THROW(
        {
            handshake::parse_handshake("500\x1e");
        },
        signalr_exception
    );

    EXPECT_THROW(
        {
            handshake::parse_handshake("true\x1e");
        },
        signalr_exception
    );

    EXPECT_THROW(
        {
            handshake::parse_handshake("null\x1e");
        },
        signalr_exception
    );
}

TEST(handshake_parse, parses_strongly_typed_error_model) {
    auto parsed_response = handshake::parse_handshake_message(
        "{\"error\":\"Protocol 'unsupported' is not supported.\"}\x1e"
    );
    const std::string remaining = std::get<0>(parsed_response);
    const handshake::handshake_response response = std::get<1>(parsed_response);

    ASSERT_TRUE(remaining.empty());
    ASSERT_TRUE(response.has_error());
    ASSERT_STREQ("Protocol 'unsupported' is not supported.", response.error.c_str());
    ASSERT_EQ(0, response.minor_version);
}
