// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "json_hub_protocol.h"
#include "test_utils.h"

using namespace signalr;

// --- Serialization Tests ---

TEST(json_hub_protocol, write_invocation_message)
{
    json_hub_protocol protocol;

    invocation_message msg1("", "Target", std::vector<value>{ value(1.0), value("Foo") });
    ASSERT_STREQ("{\"arguments\":[1,\"Foo\"],\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg1).c_str());

    invocation_message msg2("123", "Target", std::vector<value>{ value(1.0), value("Foo") });
    ASSERT_STREQ("{\"arguments\":[1,\"Foo\"],\"invocationId\":\"123\",\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg2).c_str());

    invocation_message msg3("", "Target", std::vector<value>{ value(true) });
    ASSERT_STREQ("{\"arguments\":[true],\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg3).c_str());

    invocation_message msg4("", "Target", std::vector<value>{ value(nullptr) });
    ASSERT_STREQ("{\"arguments\":[null],\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg4).c_str());

    invocation_message msg5("", "Target", std::vector<value>{});
    ASSERT_STREQ("{\"arguments\":[],\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg5).c_str());

    invocation_message msg6("", "Target", std::vector<value>{ value(std::map<std::string, value>{ {"property", value(5.0)} }) });
    ASSERT_STREQ("{\"arguments\":[{\"property\":5}],\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg6).c_str());

    invocation_message msg7("", "Target", std::vector<value>{ value(std::vector<value>{ value(1.0), value(5.0) }) });
    ASSERT_STREQ("{\"arguments\":[[1,5]],\"target\":\"Target\",\"type\":1}\x1e",
                 protocol.write_invocation_message(&msg7).c_str());
}

TEST(json_hub_protocol, write_stream_item_message)
{
    json_hub_protocol protocol;
    stream_item_message msg("1", value("stream_data"));
    ASSERT_STREQ("{\"invocationId\":\"1\",\"item\":\"stream_data\",\"type\":2}\x1e",
                 protocol.write_stream_item_message(&msg).c_str());
}

TEST(json_hub_protocol, write_completion_message)
{
    json_hub_protocol protocol;

    completion_message msg_err("1", "error_desc", value(), completion_message::result_kind::error_result);
    ASSERT_STREQ("{\"error\":\"error_desc\",\"invocationId\":\"1\",\"type\":3}\x1e",
                 protocol.write_completion_message(&msg_err).c_str());

    completion_message msg_res("1", "", value(42.0), completion_message::result_kind::non_void_result);
    ASSERT_STREQ("{\"invocationId\":\"1\",\"result\":42,\"type\":3}\x1e",
                 protocol.write_completion_message(&msg_res).c_str());

    completion_message msg_void("1", "", value(), completion_message::result_kind::void_result);
    ASSERT_STREQ("{\"invocationId\":\"1\",\"type\":3}\x1e",
                 protocol.write_completion_message(&msg_void).c_str());

    completion_message msg_null("1", "", value(nullptr), completion_message::result_kind::non_void_result);
    ASSERT_STREQ("{\"invocationId\":\"1\",\"result\":null,\"type\":3}\x1e",
                 protocol.write_completion_message(&msg_null).c_str());
}

TEST(json_hub_protocol, write_stream_invocation_message)
{
    json_hub_protocol protocol;
    stream_invocation_message msg("123", "StreamTarget", std::vector<value>{ value(1.0) });
    ASSERT_STREQ("{\"arguments\":[1],\"invocationId\":\"123\",\"target\":\"StreamTarget\",\"type\":4}\x1e",
                 protocol.write_stream_invocation_message(&msg).c_str());
}

TEST(json_hub_protocol, write_cancel_invocation_message)
{
    json_hub_protocol protocol;
    cancel_invocation_message msg("123");
    ASSERT_STREQ("{\"invocationId\":\"123\",\"type\":5}\x1e",
                 protocol.write_cancel_invocation_message(&msg).c_str());
}

TEST(json_hub_protocol, write_ping_message)
{
    json_hub_protocol protocol;
    ping_message msg;
    ASSERT_STREQ("{\"type\":6}\x1e", protocol.write_ping_message(&msg).c_str());
}

TEST(json_hub_protocol, write_close_message)
{
    json_hub_protocol protocol;

    close_message msg_plain("", false);
    ASSERT_STREQ("{\"allowReconnect\":false,\"type\":7}\x1e",
                 protocol.write_close_message(&msg_plain).c_str());

    close_message msg_err("failed connection", true);
    ASSERT_STREQ("{\"allowReconnect\":true,\"error\":\"failed connection\",\"type\":7}\x1e",
                 protocol.write_close_message(&msg_err).c_str());
}

TEST(json_hub_protocol, write_ack_message)
{
    json_hub_protocol protocol;
    ack_message msg(42);
    ASSERT_STREQ("{\"sequenceId\":42,\"type\":8}\x1e", protocol.write_ack_message(&msg).c_str());
}

TEST(json_hub_protocol, write_sequence_message)
{
    json_hub_protocol protocol;
    sequence_message msg(99);
    ASSERT_STREQ("{\"sequenceId\":99,\"type\":9}\x1e", protocol.write_sequence_message(&msg).c_str());
}

TEST(json_hub_protocol, can_serialize_binary)
{
    json_hub_protocol protocol;
    invocation_message msg("", "Target", std::vector<value>{
        value(std::vector<uint8_t>{ 0x67, 0x6F, 0x6F, 0x64, 0x20, 0x64, 0x61, 0x79 })
    });

    auto output = protocol.write_invocation_message(&msg);
    auto expected = "{\"arguments\":[\"Z29vZCBkYXk=\"],\"target\":\"Target\",\"type\":1}\x1e";
    ASSERT_STREQ(expected, output.c_str());
}

// --- Deserialization Tests ---

TEST(json_hub_protocol, parse_invocation_messages)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"arguments\":[1,\"Foo\"],\"target\":\"Target\",\"type\":1}\x1e");
    ASSERT_EQ(1, handler->invocation_messages.size());
    assert_invocation_message_equality(
        invocation_message("", "Target", std::vector<value>{ value(1.0), value("Foo") }),
        handler->invocation_messages[0]);

    parser->parse_and_handle_messages("{\"arguments\":[true],\"invocationId\":\"123\",\"target\":\"Target\",\"type\":1}\x1e");
    ASSERT_EQ(2, handler->invocation_messages.size());
    assert_invocation_message_equality(
        invocation_message("123", "Target", std::vector<value>{ value(true) }),
        handler->invocation_messages[1]);
}

TEST(json_hub_protocol, parse_stream_item_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"invocationId\":\"1\",\"item\":\"foo\",\"type\":2}\x1e");
    ASSERT_EQ(1, handler->stream_item_messages.size());
    assert_stream_item_message_equality(stream_item_message("1", value("foo")), handler->stream_item_messages[0]);
}

TEST(json_hub_protocol, parse_completion_messages)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    // Error result
    parser->parse_and_handle_messages("{\"error\":\"error\",\"invocationId\":\"1\",\"type\":3}\x1e");
    ASSERT_EQ(1, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "error", value(), completion_message::result_kind::error_result),
        handler->completion_messages[0]);

    // Non-void result
    parser->parse_and_handle_messages("{\"invocationId\":\"1\",\"result\":42,\"type\":3}\x1e");
    ASSERT_EQ(2, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "", value(42.0), completion_message::result_kind::non_void_result),
        handler->completion_messages[1]);

    // Void result
    parser->parse_and_handle_messages("{\"invocationId\":\"1\",\"type\":3}\x1e");
    ASSERT_EQ(3, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "", value(), completion_message::result_kind::void_result),
        handler->completion_messages[2]);

    // Null result
    parser->parse_and_handle_messages("{\"invocationId\":\"1\",\"result\":null,\"type\":3}\x1e");
    ASSERT_EQ(4, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "", value(nullptr), completion_message::result_kind::non_void_result),
        handler->completion_messages[3]);
}

TEST(json_hub_protocol, parse_stream_invocation_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"arguments\":[42],\"invocationId\":\"1\",\"target\":\"Stream\",\"type\":4}\x1e");
    ASSERT_EQ(1, handler->stream_invocation_messages.size());
    assert_stream_invocation_message_equality(
        stream_invocation_message("1", "Stream", std::vector<value>{ value(42.0) }),
        handler->stream_invocation_messages[0]);
}

TEST(json_hub_protocol, parse_cancel_invocation_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"invocationId\":\"1\",\"type\":5}\x1e");
    ASSERT_EQ(1, handler->cancel_invocation_messages.size());
    assert_cancel_invocation_message_equality(cancel_invocation_message("1"), handler->cancel_invocation_messages[0]);
}

TEST(json_hub_protocol, parse_ping_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"type\":6}\x1e");
    ASSERT_EQ(1, handler->ping_messages.size());
}

TEST(json_hub_protocol, parse_close_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"allowReconnect\":true,\"error\":\"server closed\",\"type\":7}\x1e");
    ASSERT_EQ(1, handler->close_messages.size());
    assert_close_message_equality(close_message("server closed", true), handler->close_messages[0]);
}

TEST(json_hub_protocol, parse_ack_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"sequenceId\":101,\"type\":8}\x1e");
    ASSERT_EQ(1, handler->ack_messages.size());
    assert_ack_message_equality(ack_message(101), handler->ack_messages[0]);
}

TEST(json_hub_protocol, parse_sequence_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"sequenceId\":202,\"type\":9}\x1e");
    ASSERT_EQ(1, handler->sequence_messages.size());
    assert_sequence_message_equality(sequence_message(202), handler->sequence_messages[0]);
}

TEST(json_hub_protocol, parsing_field_order_does_not_matter)
{
    invocation_message expected("123", "Target", std::vector<value>{ value(true) });

    const std::vector<std::string> payloads = {
        "{\"type\":1,\"invocationId\":\"123\",\"arguments\":[true],\"target\":\"Target\"}\x1e",
        "{\"target\":\"Target\",\"invocationId\":\"123\",\"type\":1,\"arguments\":[true]}\x1e",
        "{\"target\":\"Target\",\"arguments\":[true],\"type\":1,\"invocationId\":\"123\"}\x1e"
    };

    for (const auto& payload : payloads)
    {
        auto handler = std::make_shared<test_message_handler>();
        auto parser = json_hub_protocol().create_parser_and_handler(handler);
        parser->parse_and_handle_messages(payload);
        ASSERT_EQ(1, handler->invocation_messages.size());
        assert_invocation_message_equality(expected, handler->invocation_messages[0]);
    }
}

TEST(json_hub_protocol, can_parse_multiple_messages)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages(
        std::string("{\"arguments\":[],\"target\":\"Target\",\"type\":1}\x1e") +
        "{\"invocationId\":\"1\",\"result\":42,\"type\":3}\x1e"
    );

    ASSERT_EQ(1, handler->invocation_messages.size());
    ASSERT_EQ(1, handler->completion_messages.size());

    invocation_message invocation("", "Target", std::vector<value>{});
    assert_invocation_message_equality(invocation, handler->invocation_messages[0]);

    completion_message completion("1", "", value(42.0), completion_message::result_kind::non_void_result);
    assert_completion_message_equality(completion, handler->completion_messages[0]);
}

TEST(json_hub_protocol, extra_items_ignored_when_parsing)
{
    invocation_message expected("", "Target", std::vector<value>{ value(true) });
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"type\":1,\"arguments\":[true],\"target\":\"Target\",\"extra\":\"ignored\"}\x1e");
    ASSERT_EQ(1, handler->invocation_messages.size());
    assert_invocation_message_equality(expected, handler->invocation_messages[0]);
}

TEST(json_hub_protocol, unknown_message_type_is_ignored)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = json_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages("{\"type\":142}\x1e{\"type\":6}\x1e");
    ASSERT_EQ(1, handler->total_messages());
    ASSERT_EQ(1, handler->ping_messages.size());
}

// Updated invalid message table matching json_hub_protocol.cpp exceptions
std::vector<std::pair<std::string, std::string>> invalid_messages
{
    { "\x1e", "* Line 1, Column 1\n  Syntax error: value, object or array expected.\n* Line 1, Column 1\n  A valid JSON document must be either an array or an object value.\n" },
    { "foo\x1e", "* Line 1, Column 1\n  Syntax error: value, object or array expected.\n* Line 1, Column 2\n  Extra non-whitespace after JSON value.\n" },
    { "[42]\x1e", "Message was not a 'map' type" },
    { "{\x1e", "* Line 1, Column 2\n  Missing '}' or object member name\n" },

    { "{\"arguments\":[],\"target\":\"send\",\"invocationId\":42}\x1e", "Field 'type' not found" },

    { "{\"type\":1}\x1e", "Field 'target' not found for 'invocation' message" },
    { "{\"type\":1,\"target\":\"send\",\"invocationId\":42}\x1e", "Field 'arguments' not found for 'invocation' message" },
    { "{\"type\":1,\"target\":\"send\",\"arguments\":[],\"invocationId\":42}\x1e", "Expected 'invocationId' to be of type 'string'" },
    { "{\"type\":1,\"target\":\"send\",\"arguments\":42,\"invocationId\":\"42\"}\x1e", "Expected 'arguments' to be of type 'array'" },
    { "{\"type\":1,\"target\":true,\"arguments\":[],\"invocationId\":\"42\"}\x1e", "Expected 'target' to be of type 'string'" },

    { "{\"type\":3}\x1e", "Field 'invocationId' not found for 'completion' message" },
    { "{\"type\":3,\"invocationId\":42}\x1e", "Expected 'invocationId' to be of type 'string'" },
    // Updated string to match json_hub_protocol.cpp line 190
    { "{\"type\":3,\"invocationId\":\"42\",\"error\":\"foo\",\"result\":true}\x1e", "Found both 'error' and 'result' fields in 'completion' message" },

    { "{\"type\":4}\x1e", "Field 'target' not found for 'stream_invocation' message" },
    { "{\"type\":4,\"target\":\"send\"}\x1e", "Field 'arguments' not found for 'stream_invocation' message" },
    { "{\"type\":4,\"target\":123,\"arguments\":[]}\x1e", "Expected 'stream_invocation' message's 'target' to be of type 'string'" },
    { "{\"type\":4,\"target\":\"send\",\"arguments\":123}\x1e", "Expected 'stream_invocation' message's 'arguments' to be of type 'array'" },
    { "{\"type\":4,\"target\":\"send\",\"arguments\":[],\"invocationId\":123}\x1e", "Expected 'stream_invocation' message's 'invocationId' to be of type 'string'" },

    { "{\"type\":5}\x1e", "Field 'invocationId' not found for 'cancel_invocation' message" },
    { "{\"type\":5,\"invocationId\":123}\x1e", "Expected 'cancel_invocation' message's 'invocationId' to be of type 'string'" },

    { "{\"type\":8}\x1e", "Field 'sequenceId' not found in 'ack' message" },
    { "{\"type\":8,\"sequenceId\":\"not_an_int\"}\x1e", "Expected 'sequenceId' in 'ack' message to be an Int" },

    { "{\"type\":9}\x1e", "Field 'sequenceId' not found in 'sequence' message" },
    { "{\"type\":9,\"sequenceId\":\"not_an_int\"}\x1e", "Expected 'sequenceId' in 'sequence' message to be an Int" }
};

TEST(json_hub_protocol, invalid_messages_throw)
{
    for (const auto& pair : invalid_messages)
    {
        try
        {
            auto handler = std::make_shared<test_message_handler>();
            auto parser = json_hub_protocol().create_parser_and_handler(handler);
            parser->parse_and_handle_messages(pair.first);
            FAIL() << "Expected exception for payload: " << pair.first;
        }
        catch (const std::exception& exception)
        {
            ASSERT_STREQ(pair.second.c_str(), exception.what());
        }
    }
}
