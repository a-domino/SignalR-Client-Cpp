// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"

#ifdef USE_MSGPACK

#include <msgpack.hpp>

#include "binary_message_formatter.h"
#include "messagepack_hub_protocol.h"
#include "test_utils.h"
#include "signalrclient/signalr_exception.h"

using namespace signalr;

namespace
{
    std::string string_from_bytes(std::initializer_list<unsigned char> bytes)
    {
        return {reinterpret_cast<const char*>(bytes.begin()), bytes.size()};
    }
}

// --- Serialization Tests ---

TEST(messagepack_hub_protocol, write_invocation_message)
{
    messagepack_hub_protocol protocol;

    // Invocation without invocationId
    invocation_message msg1("", "Target", std::vector<value>{ value(1.0), value("Foo") });
    auto expected1 = string_from_bytes({
        0x12, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x92, 0x01, 0xA3, 0x46, 0x6F, 0x6F, 0x90
    });
    ASSERT_EQ(expected1, protocol.write_invocation_message(&msg1));

    // Invocation with invocationId
    invocation_message msg2("123", "Target", std::vector<value>{ value(1.0), value("Foo") });
    auto expected2 = string_from_bytes({
        0x15, 0x96, 0x01, 0x80, 0xA3, 0x31, 0x32, 0x33, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x92, 0x01, 0xA3, 0x46, 0x6F, 0x6F, 0x90
    });
    ASSERT_EQ(expected2, protocol.write_invocation_message(&msg2));

    // Invocation with bool argument
    invocation_message msg3("", "Target", std::vector<value>{ value(true) });
    auto expected3 = string_from_bytes({
        0x0E, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0xC3, 0x90
    });
    ASSERT_EQ(expected3, protocol.write_invocation_message(&msg3));

    // Invocation with null argument
    invocation_message msg4("", "Target", std::vector<value>{ value(nullptr) });
    auto expected4 = string_from_bytes({
        0x0E, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0xC0, 0x90
    });
    ASSERT_EQ(expected4, protocol.write_invocation_message(&msg4));

    // Invocation with no arguments
    invocation_message msg5("", "Target", std::vector<value>{});
    auto expected5 = string_from_bytes({
        0x0D, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x90, 0x90
    });
    ASSERT_EQ(expected5, protocol.write_invocation_message(&msg5));

    // Invocation with map argument
    invocation_message msg6("", "Target", std::vector<value>{ value(std::map<std::string, value>{ {"property", value(5.0)} }) });
    auto expected6 = string_from_bytes({
        0x18, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0x81, 0xA8, 0x70, 0x72, 0x6F, 0x70, 0x65, 0x72, 0x74, 0x79, 0x05, 0x90
    });
    ASSERT_EQ(expected6, protocol.write_invocation_message(&msg6));

    // Invocation with array argument
    invocation_message msg7("", "Target", std::vector<value>{ value(std::vector<value>{ value(1.0), value(5.0) }) });
    auto expected7 = string_from_bytes({
        0x10, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0x92, 0x01, 0x05, 0x90
    });
    ASSERT_EQ(expected7, protocol.write_invocation_message(&msg7));
}

TEST(messagepack_hub_protocol, write_stream_item_message)
{
    messagepack_hub_protocol protocol;
    stream_item_message msg("1", value("stream_data"));
    auto expected = string_from_bytes({
        0x11, 0x94, 0x02, 0x80, 0xA1, 0x31, 0xAB, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x5F, 0x64, 0x61, 0x74, 0x61
    });
    ASSERT_EQ(expected, protocol.write_stream_item_message(&msg));
}

TEST(messagepack_hub_protocol, write_completion_message)
{
    messagepack_hub_protocol protocol;

    // Error result
    completion_message msg_err("1", "error", value(), completion_message::result_kind::error_result);
    auto expected_err = string_from_bytes({
        0x0C, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x01, 0xA5, 0x65, 0x72, 0x72, 0x6F, 0x72
    });
    ASSERT_EQ(expected_err, protocol.write_completion_message(&msg_err));

    // Non-void result
    completion_message msg_res("1", "", value(42.0), completion_message::result_kind::non_void_result);
    auto expected_res = string_from_bytes({
        0x07, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x03, 0x2A
    });
    ASSERT_EQ(expected_res, protocol.write_completion_message(&msg_res));

    // Void result
    completion_message msg_void("1", "", value(), completion_message::result_kind::void_result);
    auto expected_void = string_from_bytes({
        0x06, 0x94, 0x03, 0x80, 0xA1, 0x31, 0x02
    });
    ASSERT_EQ(expected_void, protocol.write_completion_message(&msg_void));

    // Null non-void result
    completion_message msg_null("1", "", value(nullptr), completion_message::result_kind::non_void_result);
    auto expected_null = string_from_bytes({
        0x07, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x03, 0xC0
    });
    ASSERT_EQ(expected_null, protocol.write_completion_message(&msg_null));
}

TEST(messagepack_hub_protocol, write_cancel_invocation_message)
{
    messagepack_hub_protocol protocol;
    cancel_invocation_message msg("123");
    auto expected = string_from_bytes({
        0x07, 0x93, 0x05, 0x80, 0xA3, 0x31, 0x32, 0x33
    });
    ASSERT_EQ(expected, protocol.write_cancel_invocation_message(&msg));
}

TEST(messagepack_hub_protocol, write_ping_message)
{
    messagepack_hub_protocol protocol;
    ping_message msg;
    auto expected = string_from_bytes({
        0x02, 0x91, 0x06
    });
    ASSERT_EQ(expected, protocol.write_ping_message(&msg));
}

TEST(messagepack_hub_protocol, write_close_message)
{
    messagepack_hub_protocol protocol;

    close_message msg_plain("", false);
    auto expected_plain = string_from_bytes({
        0x04, 0x93, 0x07, 0xA0, 0xC2
    });
    ASSERT_EQ(expected_plain, protocol.write_close_message(&msg_plain));

    close_message msg_err("server closed", true);
    auto expected_err = string_from_bytes({
        0x11, 0x93, 0x07, 0xAD, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x63, 0x6C, 0x6F, 0x73, 0x65, 0x64, 0xC3
    });
    ASSERT_EQ(expected_err, protocol.write_close_message(&msg_err));
}

TEST(messagepack_hub_protocol, write_ack_message)
{
    messagepack_hub_protocol protocol;
    ack_message msg(42);
    auto expected = string_from_bytes({
        0x03, 0x92, 0x08, 0x2A
    });
    ASSERT_EQ(expected, protocol.write_ack_message(&msg));
}

TEST(messagepack_hub_protocol, write_sequence_message)
{
    messagepack_hub_protocol protocol;
    sequence_message msg(99);
    auto expected = string_from_bytes({
        0x03, 0x92, 0x09, 0x63
    });
    ASSERT_EQ(expected, protocol.write_sequence_message(&msg));
}

TEST(messagepack_hub_protocol, can_serialize_binary)
{
    messagepack_hub_protocol protocol;
    invocation_message msg("", "Target", std::vector<value>{
        value(std::vector<uint8_t>{ 23, 54, 69, 109, 200 })
    });
    auto expected = string_from_bytes({
        0x14, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0xC4, 0x05, 0x17, 0x36, 0x45, 0x6D, 0xC8, 0x90
    });
    ASSERT_EQ(expected, protocol.write_invocation_message(&msg));
}

// --- Deserialization Tests ---

TEST(messagepack_hub_protocol, parse_invocation_messages)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x12, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x92, 0x01, 0xA3, 0x46, 0x6F, 0x6F, 0x90
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->invocation_messages.size());
    assert_invocation_message_equality(
        invocation_message("", "Target", std::vector<value>{ value(1.0), value("Foo") }),
        handler->invocation_messages[0]);
}

TEST(messagepack_hub_protocol, parse_stream_item_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x11, 0x94, 0x02, 0x80, 0xA1, 0x31, 0xAB, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x5F, 0x64, 0x61, 0x74, 0x61
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->stream_item_messages.size());
    assert_stream_item_message_equality(stream_item_message("1", value("stream_data")), handler->stream_item_messages[0]);
}

TEST(messagepack_hub_protocol, parse_completion_messages)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    // Error result
    auto payload_err = string_from_bytes({
        0x0C, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x01, 0xA5, 0x65, 0x72, 0x72, 0x6F, 0x72
    });
    parser->parse_and_handle_messages(payload_err);
    ASSERT_EQ(1, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "error", value(), completion_message::result_kind::error_result),
        handler->completion_messages[0]);

    // Non-void result
    auto payload_res = string_from_bytes({
        0x07, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x03, 0x2A
    });
    parser->parse_and_handle_messages(payload_res);
    ASSERT_EQ(2, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "", value(42.0), completion_message::result_kind::non_void_result),
        handler->completion_messages[1]);

    // Void result
    auto payload_void = string_from_bytes({
        0x06, 0x94, 0x03, 0x80, 0xA1, 0x31, 0x02
    });
    parser->parse_and_handle_messages(payload_void);
    ASSERT_EQ(3, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "", value(), completion_message::result_kind::void_result),
        handler->completion_messages[2]);

    // Null result
    auto payload_null = string_from_bytes({
        0x07, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x03, 0xC0
    });
    parser->parse_and_handle_messages(payload_null);
    ASSERT_EQ(4, handler->completion_messages.size());
    assert_completion_message_equality(
        completion_message("1", "", value(nullptr), completion_message::result_kind::non_void_result),
        handler->completion_messages[3]);
}

TEST(messagepack_hub_protocol, parse_stream_invocation_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    // [4, {}, "1", "Stream", [42.0], []]
    auto payload = string_from_bytes({
        0x0F, 0x96, 0x04, 0x80, 0xA1, 0x31, 0xA6, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6D, 0x91, 0x2A, 0x90
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->stream_invocation_messages.size());
    assert_stream_invocation_message_equality(
        stream_invocation_message("1", "Stream", std::vector<value>{ value(42.0) }),
        handler->stream_invocation_messages[0]);
}

TEST(messagepack_hub_protocol, parse_cancel_invocation_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x07, 0x93, 0x05, 0x80, 0xA3, 0x31, 0x32, 0x33
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->cancel_invocation_messages.size());
    assert_cancel_invocation_message_equality(cancel_invocation_message("123"), handler->cancel_invocation_messages[0]);
}

TEST(messagepack_hub_protocol, parse_ping_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x02, 0x91, 0x06
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->ping_messages.size());
}

TEST(messagepack_hub_protocol, parse_close_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x11, 0x93, 0x07, 0xAD, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x63, 0x6C, 0x6F, 0x73, 0x65, 0x64, 0xC3
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->close_messages.size());
    assert_close_message_equality(close_message("server closed", true), handler->close_messages[0]);
}

TEST(messagepack_hub_protocol, parse_ack_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x03, 0x92, 0x08, 0x2A
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->ack_messages.size());
    assert_ack_message_equality(ack_message(42), handler->ack_messages[0]);
}

TEST(messagepack_hub_protocol, parse_sequence_message)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x03, 0x92, 0x09, 0x63
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->sequence_messages.size());
    assert_sequence_message_equality(sequence_message(99), handler->sequence_messages[0]);
}

TEST(messagepack_hub_protocol, can_parse_multiple_messages)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    auto payload = string_from_bytes({
        0x0D, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x90, 0x90,
        0x07, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x03, 0x2A
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->invocation_messages.size());
    ASSERT_EQ(1, handler->completion_messages.size());

    invocation_message invocation("", "Target", std::vector<value>{});
    assert_invocation_message_equality(invocation, handler->invocation_messages[0]);

    completion_message completion("1", "", value(42.0), completion_message::result_kind::non_void_result);
    assert_completion_message_equality(completion, handler->completion_messages[0]);
}

TEST(messagepack_hub_protocol, extra_items_ignored_when_parsing)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    invocation_message expected("", "Target", std::vector<value>{ value(1.0), value("Foo") });
    auto payload = string_from_bytes({
        0x16, 0x97, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x92, 0x01, 0xA3, 0x46, 0x6F, 0x6F, 0x90, 0xA3, 0x46, 0x6F, 0x6F
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->invocation_messages.size());
    assert_invocation_message_equality(expected, handler->invocation_messages[0]);
}

TEST(messagepack_hub_protocol, unknown_message_type_is_ignored)
{
    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    // type 110 followed by ping message (type 6)
    auto payload = string_from_bytes({
        0x04, 0x93, 0x6E, 0x80, 0xC0,
        0x02, 0x91, 0x06
    });
    parser->parse_and_handle_messages(payload);
    ASSERT_EQ(1, handler->total_messages());
    ASSERT_EQ(1, handler->ping_messages.size());
}

namespace
{
    std::vector<std::pair<std::string, std::string>> invalid_messages
    {
        { string_from_bytes({0x00}), "messagepack object was incomplete" },
        { string_from_bytes({0x01, 0xA0}), "Message was not an 'array' type" },
        { string_from_bytes({0x01, 0x90}), "Message was an empty array" },
        { string_from_bytes({0x05}), "partial messages are not supported." },
        { string_from_bytes({0x02, 0x91, 0xA0}), "reading 'type' as int failed" },
        { string_from_bytes({0x0E, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x92, 0xC0, 0x90}), "messagepack object was incomplete" },

        // invocation message
        { string_from_bytes({0x03, 0x92, 0x01, 0x80}), "invocation message has too few properties" },
        { string_from_bytes({0x0E, 0x96, 0x01, 0x80, 0x04, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0xC0, 0x90}), "reading 'invocationId' as string failed" },
        { string_from_bytes({0x0E, 0x96, 0x01, 0x80, 0xC0, 0x96, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0x91, 0xC0, 0x90}), "reading 'target' as string failed" },
        { string_from_bytes({0x0E, 0x96, 0x01, 0x80, 0xC0, 0xA6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74, 0xA1, 0xC0, 0x90}), "reading 'arguments' as array failed" },

        // completion message
        { string_from_bytes({0x07, 0x95, 0x03, 0x80, 0x91, 0x31, 0x03, 0x2A}), "reading 'invocationId' as string failed" },
        { string_from_bytes({0x07, 0x95, 0x03, 0x80, 0xA1, 0x31, 0xA0, 0x2A}), "reading 'result_kind' as int failed" },
        { string_from_bytes({0x05, 0x93, 0x03, 0x80, 0xA1, 0x31}), "completion message has too few properties" },
        { string_from_bytes({0x06, 0x94, 0x03, 0x80, 0xA1, 0x31, 0x03}), "completion result response has too few properties" },
        { string_from_bytes({0x08, 0x95, 0x03, 0x80, 0xA1, 0x31, 0x01, 0x91, 0x03}), "reading 'error' as string failed" },

        // stream item message
        { string_from_bytes({0x03, 0x92, 0x02, 0x80}), "stream item message has too few properties" },

        // cancel invocation message
        { string_from_bytes({0x03, 0x92, 0x05, 0x80}), "cancel invocation message has too few properties" },

        // TODO: test all the different close, ack & sequence message switch branches
        // close message
        { string_from_bytes({0x02, 0x91, 0x07}), "close message has too few properties" },
        { string_from_bytes({0x04, 0x93, 0x07, 0xC0, 0x90}), "reading a close message's 'allow_reconnect' field as an array is not allowed" },

        // ack message
        { string_from_bytes({0x02, 0x91, 0x08}), "ack message has too few properties" },
        { string_from_bytes({0x04, 0x92, 0x08, 0x91, 0x01}), "Can't convert ack message's SequenceId str from an array" },

        // sequence message (exercises the current copy-paste string)
        { string_from_bytes({0x02, 0x91, 0x09}), "sequence message has too few properties" },
        { string_from_bytes({0x04, 0x92, 0x09, 0x91, 0x01}), "Can't convert sequence message's SequenceId from an array" }
    };
}

TEST(messagepack_hub_protocol, invalid_messages_throw)
{
    int i = -1;
    for (const auto& pair : invalid_messages)
    {
        ++i;
        try
        {
            auto handler = std::make_shared<test_message_handler>();
            auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);
            parser->parse_and_handle_messages(pair.first);
            FAIL() << "Expected exception for payload of length: " << pair.first.size();
        }
        catch (const std::exception& exception)
        {
            ASSERT_STREQ(pair.second.c_str(), exception.what()) << "Test invalid messagepack message: " << i << std::endl;
        }
    }
}

TEST(messagepack_hub_protocol_ext, parse_invocation_message_with_ext_argument)
{
    // Frame: [1 (invocation), Headers {}, "inv123", "TargetMethod", [EXT(10, {0x01, 0x02, 0x03, 0x04})]]
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(5);
    packer.pack_int(1); // invocation
    packer.pack_map(0); // headers
    packer.pack_str(6);
    packer.pack_str_body("inv123", 6);
    packer.pack_str(12);
    packer.pack_str_body("TargetMethod", 12);

    packer.pack_array(1);
    constexpr int8_t type_code = 10;
    constexpr char ext_payload[] = {0x01, 0x02, 0x03, 0x04};
    packer.pack_ext(sizeof(ext_payload), type_code);
    packer.pack_ext_body(ext_payload, sizeof(ext_payload));

    std::string payload(sbuf.data(), sbuf.size());
    // Prefix with binary message 0x1E varint length
    binary_message_formatter::write_length_prefix(payload);
    const std::string wire_message = std::move(payload);

    const auto handler = std::make_shared<test_message_handler>();
    const auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages(wire_message);

    ASSERT_EQ(1U, handler->invocation_messages.size());
    const auto& msg = handler->invocation_messages[0];
    ASSERT_EQ("inv123", msg.invocation_id);
    ASSERT_EQ("TargetMethod", msg.target);
    ASSERT_EQ(1U, msg.arguments.size());
    ASSERT_TRUE(msg.arguments[0].is_ext());

    const auto& ext = msg.arguments[0].as_ext();
    ASSERT_EQ(10, ext.type_code);
    ASSERT_EQ((std::vector<uint8_t>{0x01, 0x02, 0x03, 0x04}), ext.data);
}

TEST(messagepack_hub_protocol_ext, parse_stream_item_message_with_ext_item)
{
    // Frame: [2 (stream_item), Headers {}, "streamId", EXT(-1, {0x11, 0x22})]
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(4);
    packer.pack_int(2); // stream_item
    packer.pack_map(0); // headers
    packer.pack_str(8);
    packer.pack_str_body("streamId", 8);

    constexpr int8_t type_code = -1; // Standard MessagePack Timestamp type code
    constexpr char ext_payload[] = {0x11, 0x22};
    packer.pack_ext(sizeof(ext_payload), type_code);
    packer.pack_ext_body(ext_payload, sizeof(ext_payload));

    std::string payload(sbuf.data(), sbuf.size());
    binary_message_formatter::write_length_prefix(payload);

    auto handler = std::make_shared<test_message_handler>();
    const auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages(payload);

    ASSERT_EQ(1U, handler->stream_item_messages.size());
    const auto& msg = handler->stream_item_messages[0];
    ASSERT_EQ("streamId", msg.invocation_id);
    ASSERT_TRUE(msg.item.is_ext());
    ASSERT_EQ(-1, msg.item.as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0x11, 0x22}), msg.item.as_ext().data);
}

TEST(messagepack_hub_protocol_ext, parse_completion_message_with_ext_result)
{
    // Frame: [3 (completion), Headers {}, "inv456", 1 (non_void_result), EXT(5, {0xAA, 0xBB})]
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(5);
    packer.pack_int(3); // completion
    packer.pack_map(0); // headers
    packer.pack_str(6);
    packer.pack_str_body("inv456", 6);
    packer.pack_int(static_cast<int>(completion_message::result_kind::non_void_result));

    constexpr int8_t type_code = 5;
    constexpr char ext_payload[] = {static_cast<char>(0xAA), static_cast<char>(0xBB)};
    packer.pack_ext(sizeof(ext_payload), type_code);
    packer.pack_ext_body(ext_payload, sizeof(ext_payload));

    std::string payload(sbuf.data(), sbuf.size());
    binary_message_formatter::write_length_prefix(payload);

    const auto handler = std::make_shared<test_message_handler>();
    const auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages(payload);

    ASSERT_EQ(1U, handler->completion_messages.size());
    const auto& msg = handler->completion_messages[0];
    ASSERT_EQ("inv456", msg.invocation_id);
    ASSERT_TRUE(msg.result.is_ext());
    ASSERT_EQ(5, msg.result.as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0xAA, 0xBB}), msg.result.as_ext().data);
}

TEST(messagepack_hub_protocol_ext, write_invocation_message_with_ext_argument_roundtrips)
{
    messagepack_hub_protocol protocol;
    constexpr int8_t type_code = 12;
    std::vector<uint8_t> ext_data{0x10, 0x20, 0x30, 0x40, 0x50};
    value ext_value(type_code, ext_data);

    invocation_message original_msg("req1", "ProcessExt", std::vector<value>{ext_value});
    auto wire_bytes = protocol.write_invocation_message(&original_msg);

    auto handler = std::make_shared<test_message_handler>();
    const auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    parser->parse_and_handle_messages(wire_bytes);

    ASSERT_EQ(1U, handler->invocation_messages.size());
    const auto& parsed_msg = handler->invocation_messages[0];
    ASSERT_EQ("req1", parsed_msg.invocation_id);
    ASSERT_EQ("ProcessExt", parsed_msg.target);
    ASSERT_EQ(1U, parsed_msg.arguments.size());
    ASSERT_TRUE(parsed_msg.arguments[0].is_ext());
    ASSERT_EQ(type_code, parsed_msg.arguments[0].as_ext().type_code);
    ASSERT_EQ(ext_data, parsed_msg.arguments[0].as_ext().data);
}

TEST(messagepack_hub_protocol_ext, ext_in_close_message_allow_reconnect_throws)
{
    // [7 (close), "error", EXT(0, {})]
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(3);
    packer.pack_int(7);
    packer.pack_str(5);
    packer.pack_str_body("error", 5);
    packer.pack_ext(0, 0);

    std::string payload(sbuf.data(), sbuf.size());
    binary_message_formatter::write_length_prefix(payload);

    auto handler = std::make_shared<test_message_handler>();
    const auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    ASSERT_THROW(parser->parse_and_handle_messages(payload), signalr::signalr_exception);
}

TEST(messagepack_hub_protocol_ext_edge_cases, roundtrip_zero_length_ext_payload)
{
    messagepack_hub_protocol protocol;
    value ext_val(7, std::vector<uint8_t>{});

    invocation_message msg("inv0", "EmptyExtTarget", std::vector<value>{ext_val});
    auto wire_bytes = protocol.write_invocation_message(&msg);

    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);
    parser->parse_and_handle_messages(wire_bytes);

    ASSERT_EQ(1U, handler->invocation_messages.size());
    const auto& parsed = handler->invocation_messages[0];
    ASSERT_TRUE(parsed.arguments[0].is_ext());
    ASSERT_EQ(7, parsed.arguments[0].as_ext().type_code);
    ASSERT_TRUE(parsed.arguments[0].as_ext().data.empty());
}

TEST(messagepack_hub_protocol_ext_edge_cases, parse_all_msgpack_fixext_sizes)
{
    // Tests fixext 1 (0xd4), fixext 2 (0xd5), fixext 4 (0xd6), fixext 8 (0xd7), fixext 16 (0xd8)
    const std::vector<size_t> fixext_sizes = {1, 2, 4, 8, 16};

    for (size_t sz : fixext_sizes)
    {
        msgpack::sbuffer sbuf;
        msgpack::packer<msgpack::sbuffer> packer(sbuf);

        packer.pack_array(5);
        packer.pack_int(1); // invocation
        packer.pack_map(0); // headers
        packer.pack_str(4);
        packer.pack_str_body("test", 4);
        packer.pack_str(6);
        packer.pack_str_body("Target", 6);

        packer.pack_array(1);
        std::vector<uint8_t> payload(sz, static_cast<uint8_t>(sz));
        packer.pack_ext(sz, static_cast<int8_t>(sz));
        packer.pack_ext_body(reinterpret_cast<const char*>(payload.data()), sz);

        std::string wire_message(sbuf.data(), sbuf.size());
        binary_message_formatter::write_length_prefix(wire_message);

        auto handler = std::make_shared<test_message_handler>();
        auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);
        parser->parse_and_handle_messages(wire_message);

        ASSERT_EQ(1U, handler->invocation_messages.size());
        const auto& arg = handler->invocation_messages[0].arguments[0];
        ASSERT_TRUE(arg.is_ext());
        ASSERT_EQ(static_cast<int8_t>(sz), arg.as_ext().type_code);
        ASSERT_EQ(payload, arg.as_ext().data);
    }
}

TEST(messagepack_hub_protocol_ext_edge_cases, roundtrip_large_ext_16_payload)
{
    messagepack_hub_protocol protocol;
    // 300 bytes forces the 'ext 16' (0xc8) variable format (> 255 bytes)
    std::vector<uint8_t> large_data(300);
    for (size_t i = 0; i < large_data.size(); ++i)
    {
        large_data[i] = static_cast<uint8_t>(i % 256);
    }

    value ext_val(42, large_data);
    invocation_message original_msg("large_inv", "HandleLargeExt", std::vector<value>{ext_val});
    auto wire_bytes = protocol.write_invocation_message(&original_msg);

    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);
    parser->parse_and_handle_messages(wire_bytes);

    ASSERT_EQ(1U, handler->invocation_messages.size());
    const auto& parsed = handler->invocation_messages[0];
    ASSERT_TRUE(parsed.arguments[0].is_ext());
    ASSERT_EQ(42, parsed.arguments[0].as_ext().type_code);
    ASSERT_EQ(large_data, parsed.arguments[0].as_ext().data);
}

TEST(messagepack_hub_protocol_ext_edge_cases, parse_ext_nested_in_array_and_map)
{
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(5);
    packer.pack_int(1); // invocation
    packer.pack_map(0); // headers
    packer.pack_str(4);
    packer.pack_str_body("nest", 4);
    packer.pack_str(6);
    packer.pack_str_body("Target", 6);

    // Arguments: [ [EXT(1, {0x11})], {"map_key": EXT(2, {0x22})} ]
    packer.pack_array(2);

    // Nested array
    packer.pack_array(1);
    constexpr char ext1_body[] = {0x11};
    packer.pack_ext(sizeof(ext1_body), 1);
    packer.pack_ext_body(ext1_body, sizeof(ext1_body));

    // Nested map
    packer.pack_map(1);
    packer.pack_str(7);
    packer.pack_str_body("map_key", 7);
    constexpr char ext2_body[] = {0x22};
    packer.pack_ext(sizeof(ext2_body), 2);
    packer.pack_ext_body(ext2_body, sizeof(ext2_body));

    std::string payload(sbuf.data(), sbuf.size());
    binary_message_formatter::write_length_prefix(payload);

    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);
    parser->parse_and_handle_messages(payload);

    ASSERT_EQ(1U, handler->invocation_messages.size());
    const auto& args = handler->invocation_messages[0].arguments;
    ASSERT_EQ(2U, args.size());

    // Verify nested array
    ASSERT_TRUE(args[0].is_array());
    ASSERT_EQ(1U, args[0].as_array().size());
    ASSERT_TRUE(args[0].as_array()[0].is_ext());
    ASSERT_EQ(1, args[0].as_array()[0].as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0x11}), args[0].as_array()[0].as_ext().data);

    // Verify nested map
    ASSERT_TRUE(args[1].is_map());
    const auto& map = args[1].as_map();
    ASSERT_EQ(1U, map.count("map_key"));
    ASSERT_TRUE(map.at("map_key").is_ext());
    ASSERT_EQ(2, map.at("map_key").as_ext().type_code);
    ASSERT_EQ((std::vector<uint8_t>{0x22}), map.at("map_key").as_ext().data);
}

TEST(messagepack_hub_protocol_ext_edge_cases, truncated_ext_body_throws_exception)
{
    // Handcraft a message claiming a 4-byte fixext (0xd6), but providing only 2 bytes
    auto truncated = string_from_bytes({
        0x0D, // Length prefix
        0x95, 0x01, 0x80, 0xA1, 0x31, 0xA1, 0x54, 0x91,
        0xD6, 0x05, 0xAA, 0xBB // 0xD6 is fixext 4 (requires type_code + 4 data bytes), only 2 data bytes provided
    });

    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    ASSERT_THROW(parser->parse_and_handle_messages(truncated), signalr_exception);
}

TEST(messagepack_hub_protocol_ext_edge_cases, ext_in_invocation_id_throws_exception)
{
    // [1 (invocation), Headers {}, EXT(0, {0x01}) (invalid invocationId), "target", []]
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(5);
    packer.pack_int(1);
    packer.pack_map(0);
    packer.pack_ext(1, 0);
    constexpr char b = 0x01;
    packer.pack_ext_body(&b, 1);
    packer.pack_str(6);
    packer.pack_str_body("target", 6);
    packer.pack_array(0);

    std::string payload(sbuf.data(), sbuf.size());
    binary_message_formatter::write_length_prefix(payload);

    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    ASSERT_THROW(parser->parse_and_handle_messages(payload), signalr_exception);
}

TEST(messagepack_hub_protocol_ext_edge_cases, ext_in_completion_result_kind_throws_exception)
{
    // [3 (completion), Headers {}, "inv1", EXT(0, {0x01}) (invalid result_kind)]
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(sbuf);

    packer.pack_array(4);
    packer.pack_int(3);
    packer.pack_map(0);
    packer.pack_str(4);
    packer.pack_str_body("inv1", 4);
    packer.pack_ext(1, 0);
    constexpr char b = 0x01;
    packer.pack_ext_body(&b, 1);

    std::string payload(sbuf.data(), sbuf.size());
    binary_message_formatter::write_length_prefix(payload);

    auto handler = std::make_shared<test_message_handler>();
    auto parser = messagepack_hub_protocol().create_parser_and_handler(handler);

    ASSERT_THROW(parser->parse_and_handle_messages(payload), signalr_exception);
}

#endif
