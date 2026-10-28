// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"

#ifdef USE_MSGPACK
#include "messagepack_hub_protocol.h"
#include "message_type.h"
#include "signalrclient/signalr_exception.h"
#include <msgpack.hpp>
#include "binary_message_parser.h"
#include "binary_message_formatter.h"
#include <cmath>

namespace {
    class writer_interface {
    public:
        virtual writer_interface& write(const char* buf, size_t s) = 0;
        virtual ~writer_interface() = default;
    };

    // replaces use of msgpack::sbuffer when packing so that we can write directly to the string instead of writing to
    // another buffer and copying to a string later
    class string_wrapper : writer_interface
    {
    public:
        std::string str;

        string_wrapper& write(const char* buf, size_t s) override
        {
            str.append(buf, s);
            return *this;
        }
    };
}

static void pack_string(msgpack::packer<string_wrapper>& packer, const std::string& str) {
    if (str.empty())
    {
        packer.pack_nil();
    }
    else
    {
        packer.pack_str(static_cast<uint32_t>(str.length()));
        packer.pack_str_body(str.data(), static_cast<uint32_t>(str.length()));
    }
}

static void pack_signalr_value(const signalr::value& v, msgpack::packer<string_wrapper>& packer) {
    switch (v.type())
    {
    case signalr::value_type::boolean:
    {
        if (v.as_bool())
        {
            packer.pack_true();
        }
        else
        {
            packer.pack_false();
        }
        return;
    }
    case signalr::value_type::float64:
    {
        const auto value = v.as_double();
        double intPart;
        // Workaround for 1.0 being output as 1.0 instead of 1
        // because the server expects certain values to be 1 instead of 1.0 (like protocol version)
        if (std::modf(value, &intPart) == 0)
        {
            if (value < 0)
            {
                if (value >= static_cast<double>(INT64_MIN))
                {
                    // Fits within int64_t
                    packer.pack_int64(static_cast<int64_t>(intPart));
                    return;
                }
                // Remain as double
                packer.pack_double(value);
                return;
            }
            if (value <= static_cast<double>(UINT64_MAX))
            {
                // Fits within uint64_t
                packer.pack_uint64(static_cast<uint64_t>(intPart));
                return;
            }
            // Remain as double
            packer.pack_double(value);
            return;
        }
        packer.pack_double(v.as_double());
        return;
    }
    case signalr::value_type::string:
    {
        const auto length = v.as_string().length();
        packer.pack_str(static_cast<uint32_t>(length));
        packer.pack_str_body(v.as_string().data(), static_cast<uint32_t>(length));
        return;
    }
    case signalr::value_type::array:
    {
        const auto& array = v.as_array();
        packer.pack_array(static_cast<uint32_t>(array.size()));
        for (auto& val : array)
        {
            pack_signalr_value(val, packer);
        }
        return;
    }
    case signalr::value_type::map:
    {
        const auto& obj = v.as_map();
        packer.pack_map(static_cast<uint32_t>(obj.size()));
        for (auto& val : obj)
        {
            packer.pack_str(static_cast<uint32_t>(val.first.size()));
            packer.pack_str_body(val.first.data(), static_cast<uint32_t>(val.first.size()));
            pack_signalr_value(val.second, packer);
        }
        return;
    }
    case signalr::value_type::binary:
    {
        const auto& bin = v.as_binary();
        packer.pack_bin(static_cast<uint32_t>(bin.size()));
        packer.pack_bin_body(reinterpret_cast<const char*>(bin.data()), static_cast<uint32_t>(bin.size()));
        return;
    }
    case signalr::value_type::null:
    default:
        packer.pack_nil();
    }
}

template<typename invocation_type>
static std::string write_invocation_msg(const invocation_type* invocation) {
    // Invocation & Stream Invocation messages have the following structure:
    //   -> [1, Headers, InvocationId, Target, [Arguments], [StreamIds]]
    string_wrapper str;
    msgpack::packer<string_wrapper> packer(str);
    packer.pack_array(6);

    packer.pack_int(static_cast<int>(signalr::message_type::invocation));
    // Headers
    packer.pack_map(0);

    pack_string(packer, invocation->invocation_id);

    pack_string(packer, invocation->target);

    packer.pack_array(static_cast<uint32_t>(invocation->arguments.size()));
    for (auto& val : invocation->arguments)
    {
        pack_signalr_value(val, packer);
    }

    // TODO: StreamIds
    packer.pack_array(0);

    signalr::binary_message_formatter::write_length_prefix(str.str);
    return std::move(str.str);
}

template<typename seq_type>
static std::string write_seq_msg(const seq_type* seq_msg) {
    // messages have the following structure:
    //   -> [8 or 9, SequenceId]
    string_wrapper str;
    msgpack::packer<string_wrapper> packer(str);
    packer.pack_array(2);

    packer.pack_int(static_cast<int>(seq_type::msg_type));

    packer.pack_int64(seq_msg->sequence_id);

    signalr::binary_message_formatter::write_length_prefix(str.str);
    return std::move(str.str);
}

namespace signalr
{
    const std::string messagepack_hub_protocol::m_protocol_name = "messagepack";

    static signalr::value createValue(const msgpack::object& v)
    {
        switch (v.type)
        {
        case msgpack::type::object_type::BOOLEAN:
            return {v.via.boolean};
        case msgpack::type::object_type::FLOAT64:
        case msgpack::type::object_type::FLOAT32:
            return {v.via.f64};
        case msgpack::type::object_type::POSITIVE_INTEGER:
            return {static_cast<double>(v.via.u64)};
        case msgpack::type::object_type::NEGATIVE_INTEGER:
            return {static_cast<double>(v.via.i64)};
        case msgpack::type::object_type::STR:
            return {v.via.str.ptr, v.via.str.size};
        case msgpack::type::object_type::ARRAY:
        {
            std::vector<signalr::value> vec;
            for (size_t i = 0; i < v.via.array.size; ++i)
            {
                vec.push_back(createValue(*(v.via.array.ptr + i)));
            }
            return {std::move(vec)};
        }
        case msgpack::type::object_type::MAP:
        {
            std::map<std::string, value> map;
            for (size_t i = 0; i < v.via.map.size; ++i)
            {
                auto key = (v.via.map.ptr + i)->key.as<std::string>();
                map.insert({ key, createValue((v.via.map.ptr + i)->val) });
            }
            return {std::move(map)};
        }
        case msgpack::type::object_type::BIN:
        {
            std::vector<uint8_t> vec = std::vector<uint8_t>(v.via.bin.ptr, v.via.bin.ptr + v.via.bin.size);
            return {std::move(vec)};
        }
        case msgpack::type::object_type::EXT:
            throw signalr_exception("messagepack type 'EXT' not supported");
        case msgpack::type::object_type::NIL:
        default:
            return {};
        }
    }

#define WRITE_MESSAGE(hub_message_type, hub_message) \
    write_ ## hub_message_type(static_cast<hub_message_type const*>(hub_message))

    template <signalr::message_type message_type>
    std::string messagepack_hub_protocol::write_message(const hub_message<message_type> *const hub_message) const {
        switch (message_type) {
        case signalr::message_type::invocation:
            return std::move(WRITE_MESSAGE(invocation_message, hub_message));
        case signalr::message_type::stream_item:
            return std::move(WRITE_MESSAGE(stream_item_message, hub_message));
        case signalr::message_type::completion:
            return std::move(WRITE_MESSAGE(completion_message, hub_message));
        case signalr::message_type::stream_invocation:
            return std::move(WRITE_MESSAGE(stream_invocation_message, hub_message));
        case signalr::message_type::cancel_invocation:
            return std::move(WRITE_MESSAGE(cancel_invocation_message, hub_message));
        case signalr::message_type::ping:
            return std::move(WRITE_MESSAGE(ping_message, hub_message));
        case signalr::message_type::close:
            return std::move(WRITE_MESSAGE(close_message, hub_message));
        case signalr::message_type::ack:
            return std::move(WRITE_MESSAGE(ack_message, hub_message));
        case signalr::message_type::sequence:
            return std::move(WRITE_MESSAGE(sequence_message, hub_message));
        default:
            string_wrapper str;
            // ReSharper disable once CppDeclaratorNeverUsed
            msgpack::packer<string_wrapper> packer(str); // done for side-effects...
            binary_message_formatter::write_length_prefix(str.str);
            return std::move(str.str);
        }
    }

    std::string messagepack_hub_protocol::write_invocation_message(const invocation_message* invocation) const {
        return std::move(write_invocation_msg<invocation_message>(invocation));
    }

    std::string messagepack_hub_protocol::write_stream_item_message(const stream_item_message* stream_item) const {
        // StreamItem messages have the following structure:
        //   -> [2, Headers, InvocationId, Item]
        string_wrapper str;
        msgpack::packer<string_wrapper> packer(str);
        packer.pack_array(4);

        // Pack message type (i.e., 2)
        packer.pack_int(static_cast<int>(message_type::stream_item));

        // Pack Headers
        packer.pack_map(0);

        // Pack InvocationId
        pack_string(packer, stream_item->invocation_id);

        // Pack Item
        pack_signalr_value(stream_item->item, packer);

        binary_message_formatter::write_length_prefix(str.str);
        return std::move(str.str);
    }

    std::string messagepack_hub_protocol::write_completion_message(const completion_message* completion) const {
        // Completion message have the following structure:
        //   -> [3, Headers, InvocationId, ResultKind, Result?]
        string_wrapper str;
        msgpack::packer<string_wrapper> packer(str);

        if (completion->kind_of_result == completion_message::result_kind::void_result) {
            packer.pack_array(4);
        } else {
            packer.pack_array(5);
        }

        packer.pack_int(static_cast<int>(message_type::completion));

        // Headers
        packer.pack_map(0);

        // Pack InvocationId
        pack_string(packer, completion->invocation_id);

        packer.pack_int(static_cast<int>(completion->kind_of_result));
        switch (completion->kind_of_result)
        {
        case completion_message::result_kind::error_result:
            pack_string(packer, completion->error);
            break;
        case completion_message::result_kind::non_void_result:
            pack_signalr_value(completion->result, packer);
            break;
        case completion_message::result_kind::void_result:
        default: ;
        }

        binary_message_formatter::write_length_prefix(str.str);
        return std::move(str.str);
    }

    std::string messagepack_hub_protocol::write_stream_invocation_message(
        const stream_invocation_message* invocation
    ) const {
        return std::move(write_invocation_msg<stream_invocation_message>(invocation));
    }

    std::string messagepack_hub_protocol::write_cancel_invocation_message(
        const cancel_invocation_message* cancel_msg
    ) const {
        // CancelInvocation messages have the following structure:
        //   -> [5, Headers, InvocationId]
        string_wrapper str;
        msgpack::packer<string_wrapper> packer(str);
        packer.pack_array(3);

        packer.pack_int(static_cast<int>(message_type::cancel_invocation));

        // Pack Headers
        packer.pack_map(0);

        // Pack InvocationId
        pack_string(packer, cancel_msg->invocation_id);

        binary_message_formatter::write_length_prefix(str.str);
        return std::move(str.str);
    }

    std::string messagepack_hub_protocol::write_ping_message(const ping_message*) const {
        string_wrapper str;
        msgpack::packer<string_wrapper> packer(str);

        packer.pack_array(1);
        packer.pack_int(static_cast<int>(message_type::ping));

        binary_message_formatter::write_length_prefix(str.str);
        return str.str;
    }

    // TODO: When this client is closing, a close message should be sent...
    std::string messagepack_hub_protocol::write_close_message(const close_message* close_msg) const {
        // Close messages have the following structure:
        //   -> [7, Error, AllowReconnect?]
        string_wrapper str;
        msgpack::packer<string_wrapper> packer(str);

        packer.pack_array(3);

        packer.pack_int(static_cast<int>(message_type::close));

        // Pack Error
        pack_string(packer, close_msg->error);

        // Pack Allow Reconnect:
        if (close_msg->allow_reconnect) {
            packer.pack_true();
        } else {
            packer.pack_false();
        }

        binary_message_formatter::write_length_prefix(str.str);
        return str.str;
    }

    std::string messagepack_hub_protocol::write_ack_message(const ack_message* msg) const {
        return std::move(write_seq_msg<ack_message>(msg));
    }

    std::string messagepack_hub_protocol::write_sequence_message(const sequence_message* msg) const {
        return std::move(write_seq_msg<sequence_message>(msg));
    }

    void messagepack_hub_protocol::parser_and_handler::parse_and_handle_messages(const std::string& message) const
    {
        size_t length_prefix_length;
        size_t length_of_message;
        const char* remaining_message = message.data();
        size_t remaining_message_length = message.length();

        while (
            binary_message_parser::try_parse_message(
                reinterpret_cast<const unsigned char*>(remaining_message),
                remaining_message_length, &length_prefix_length, &length_of_message
            )
        ) {
            assert(length_prefix_length <= remaining_message_length);
            remaining_message += length_prefix_length;
            remaining_message_length -= length_prefix_length;
            assert(remaining_message_length >= length_of_message);

            msgpack::unpacker pac;
            pac.reserve_buffer(length_of_message);
            memcpy(pac.buffer(), remaining_message, length_of_message);
            pac.buffer_consumed(length_of_message);
            msgpack::object_handle obj_handle;

            if (!pac.next(obj_handle))
            {
                throw signalr_exception("messagepack object was incomplete");
            }

            auto& msgpack_obj = obj_handle.get();

            if (msgpack_obj.type != msgpack::type::ARRAY)
            {
                throw signalr_exception("Message was not an 'array' type");
            }

            auto num_elements_of_message = msgpack_obj.via.array.size;
            if (msgpack_obj.via.array.size == 0)
            {
                throw signalr_exception("Message was an empty array");
            }

            auto msgpack_obj_index = msgpack_obj.via.array.ptr;
            if (msgpack_obj_index->type != msgpack::type::POSITIVE_INTEGER)
            {
                throw signalr_exception("reading 'type' as int failed");
            }
            auto int_type = msgpack_obj_index->via.i64;
            auto type = static_cast<message_type>(int_type);
            ++msgpack_obj_index;

            switch (type)
            {
            case message_type::invocation:
            case message_type::stream_invocation:
            {
                // Invocation messages have the following structure:
                //   -> [1, Headers, InvocationId, Target, [Arguments], [StreamIds]]
                if (num_elements_of_message < 5)
                {
                    throw signalr_exception("invocation message has too few properties");
                }

                // HEADERS
                ++msgpack_obj_index;

                std::string invocation_id;
                if (msgpack_obj_index->type == msgpack::type::STR)
                {
                    invocation_id.append(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                }
                else if (msgpack_obj_index->type != msgpack::type::NIL)
                {
                    throw signalr_exception("reading 'invocationId' as string failed");
                }
                ++msgpack_obj_index;

                if (msgpack_obj_index->type != msgpack::type::STR)
                {
                    throw signalr_exception("reading 'target' as string failed");
                }

                std::string target(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                ++msgpack_obj_index;

                if (msgpack_obj_index->type != msgpack::type::ARRAY)
                {
                    throw signalr_exception("reading 'arguments' as array failed");
                }

                std::vector<signalr::value> args;
                auto size = msgpack_obj_index->via.array.size;
                auto arg_array_index = msgpack_obj_index->via.array.ptr;
                for (uint32_t i = 0; i < size; ++i)
                {
                    args.emplace_back(createValue(*arg_array_index));
                    ++arg_array_index;
                }

                if (num_elements_of_message > 5)
                {
                    // This is for the StreamIds when they are supported
                    // TODO: add StreamId support...
                    ++msgpack_obj_index;
                }

                if (type == message_type::invocation) {
                    m_message_handler->handle_invocation_message(
                        std::move(invocation_message(std::move(invocation_id), std::move(target), std::move(args)))
                    );
                } else {
                    m_message_handler->handle_stream_invocation_message(
                        std::move(
                            stream_invocation_message(std::move(invocation_id), std::move(target), std::move(args))
                        )
                    );
                }

                break;
            }
            case message_type::completion:
            {
                // Completion message have the following structure:
                //   -> [3, Headers, InvocationId, ResultKind, Result?]
                if (num_elements_of_message < 4)
                {
                    throw signalr_exception("completion message has too few properties");
                }

                // HEADERS
                ++msgpack_obj_index;

                if (msgpack_obj_index->type != msgpack::type::STR)
                {
                    throw signalr_exception("reading 'invocationId' as string failed");
                }
                std::string invocation_id(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                ++msgpack_obj_index;

                if (msgpack_obj_index->type != msgpack::type::POSITIVE_INTEGER)
                {
                    throw signalr_exception("reading 'result_kind' as int failed");
                }
                const auto result_kind = static_cast<completion_message::result_kind>(msgpack_obj_index->via.i64);
                ++msgpack_obj_index;

                std::string error;
                signalr::value result;
                switch (result_kind) {
                case completion_message::result_kind::error_result:
                    if (num_elements_of_message < 5) {
                        throw signalr_exception("completion error response has too few properties");
                    }
                    if (msgpack_obj_index->type != msgpack::type::STR)
                    {
                        throw signalr_exception("reading 'error' as string failed");
                    }
                    error.append(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                    break;
                case completion_message::result_kind::void_result:
                    break;
                case completion_message::result_kind::non_void_result:
                    if (num_elements_of_message < 5) {
                        throw signalr_exception("completion result response has too few properties");
                    }
                    result = createValue(*msgpack_obj_index);
                    break;
                default:
                    throw signalr_exception(
                        "Unknown completion result type: " + std::to_string(static_cast<int>(result_kind))
                    );
                }

                m_message_handler->handle_completion_message(
                    std::move(
                        completion_message(
                            std::move(invocation_id), std::move(error), std::move(result), result_kind
                        )
                    )
                );
                break;
            }
            case message_type::ping:
                m_message_handler->handle_ping_message(std::move(ping_message()));
                break;
            case message_type::stream_item: {
                // StreamItem messages have the following structure:
                //   -> [2, Headers, InvocationId, Item]
                if (num_elements_of_message < 4) {
                    throw signalr_exception("stream item message has too few properties");
                }

                // HEADERS
                ++msgpack_obj_index;

                // InvocationId
                if (msgpack_obj_index->type != msgpack::type::STR)
                {
                    throw signalr_exception("reading stream item message 'invocationId' as string failed");
                }
                std::string invocation_id(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                ++msgpack_obj_index;

                // Item
                signalr::value item = createValue(*msgpack_obj_index);


                m_message_handler->handle_stream_item_message(
                    std::move(stream_item_message(std::move(invocation_id), std::move(item)))
                );
                break;
            }
            case message_type::cancel_invocation: {
                // CancelInvocation messages have the following structure:
                //   -> [5, Headers, InvocationId]
                if (num_elements_of_message < 3) {
                    throw signalr_exception("cancel invocation message has too few properties");
                }

                // HEADERS
                ++msgpack_obj_index;

                // InvocationId
                if (msgpack_obj_index->type != msgpack::type::STR)
                {
                    throw signalr_exception("reading stream item message 'invocationId' as string failed");
                }
                std::string invocation_id(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);

                m_message_handler->handle_cancel_invocation_message(
                    std::move(cancel_invocation_message(std::move(invocation_id)))
                );
                break;
            }
            case message_type::close: {
                // Close messages have the following structure:
                //   -> [7, Error, AllowReconnect?]
                if (num_elements_of_message < 2) {
                    throw signalr_exception("close message has too few properties");
                }

                // Error
                if (msgpack_obj_index->type != msgpack::type::STR)
                {
                    throw signalr_exception("reading stream item message 'invocationId' as string failed");
                }
                std::string error(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);

                bool allow_reconnect = false;
                if (num_elements_of_message > 2) {
                    ++msgpack_obj_index;
                    switch (msgpack_obj_index->type) {
                    case msgpack::type::STR:
                        {
                            std::string allow_reconnect_str(
                                msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size
                            );
                            std::transform(
                                allow_reconnect_str.begin(), allow_reconnect_str.end(), allow_reconnect_str.begin(),
                                [](const unsigned char c) { return std::tolower(c); }
                            );
                            allow_reconnect = allow_reconnect_str == "true" || allow_reconnect_str == "1" ||
                                allow_reconnect_str == "yes" || allow_reconnect_str == "y";
                        }
                        break;
                    case msgpack::type::NIL: // false, so do nothing...
                        break;
                    case msgpack::type::BOOLEAN:
                        allow_reconnect = msgpack_obj_index->via.boolean;
                        break;
                    case msgpack::type::POSITIVE_INTEGER:
                        allow_reconnect = msgpack_obj_index->via.i64 != 0;
                        break;
                    case msgpack::type::NEGATIVE_INTEGER:
                        allow_reconnect = msgpack_obj_index->via.u64 != 0;
                        break;
                    case msgpack::type::FLOAT32:
                    case msgpack::type::FLOAT64:
                        allow_reconnect = msgpack_obj_index->via.f64 != 0;
                        break;
                    case msgpack::type::BIN:
                        break;
                    case msgpack::type::ARRAY:
                        throw signalr_exception(
                            "reading a close message's 'allow_reconnect' field as an array is not allowed"
                        );
                        break;
                    case msgpack::type::MAP:
                        throw signalr_exception(
                            "reading a close message's 'allow_reconnect' field as a map is not allowed"
                        );
                        break;
                    case msgpack::type::EXT:
                        throw signalr_exception(
                            "reading a close message's 'allow_reconnect' field as EXT is not allowed"
                        );
                        break;
                    default:
                        throw signalr_exception(
                            "encountered unknown type while reading a close message's 'allow_reconnect' field: " +
                            std::to_string(msgpack_obj_index->type)
                        );
                    }
                }

                m_message_handler->handle_close_message(
                    std::move(close_message(std::move(error), allow_reconnect))
                );
                break;
            }
            case message_type::ack: {
                // Ack messages have the following structure:
                //   -> [8, SequenceId]
                if (num_elements_of_message < 2) {
                    throw signalr_exception("ack message has too few properties");
                }

                int64_t sequency_id = 0;
                switch (msgpack_obj_index->type) {
                case msgpack::type::STR:
                    {
                        std::string invocation_id_str(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                        try {
                            sequency_id = std::stoll(invocation_id_str);
                        } catch (...) {
                            throw signalr_exception("ack message's SequenceId str isn't an int");
                        }
                    }
                case msgpack::type::NIL:
                    break;
                case msgpack::type::BOOLEAN:
                    sequency_id = msgpack_obj_index->via.boolean ? 1 : 0;
                    break;
                case msgpack::type::POSITIVE_INTEGER:
                case msgpack::type::NEGATIVE_INTEGER:
                    sequency_id = msgpack_obj_index->via.i64;
                    break;
                case msgpack::type::FLOAT32:
                case msgpack::type::FLOAT64:
                    sequency_id = static_cast<int64_t>(msgpack_obj_index->via.f64);
                    break;
                case msgpack::type::BIN:
                    throw signalr_exception("Can't parse ack message's SequenceId str from binary");
                    break;
                case msgpack::type::ARRAY:
                    throw signalr_exception("Can't convert ack message's SequenceId str from an array");
                    break;
                case msgpack::type::MAP:
                    throw signalr_exception("Can't convert ack message's SequenceId str from a map");
                    break;
                case msgpack::type::EXT:
                    throw signalr_exception("Can't convert ack message's SequenceId str from EXT type");
                    break;
                default:
                    throw signalr_exception(
                        "encountered unknown type while reading an ack message's 'SequenceId' field: " +
                        std::to_string(msgpack_obj_index->type)
                    );
                }

                m_message_handler->handle_ack_message(std::move(ack_message(sequency_id)));
                break;
            }
            case message_type::sequence: {
                // And Sequence messages have the following structure:
                //   -> [8, SequenceId]
                if (num_elements_of_message < 2) {
                    throw signalr_exception("ack message has too few properties");
                }

                int64_t sequency_id = 0;
                switch (msgpack_obj_index->type) {
                case msgpack::type::STR:
                    {
                        std::string invocation_id_str(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                        try {
                            sequency_id = std::stoll(invocation_id_str);
                        } catch (...) {
                            throw signalr_exception("sequence message's SequenceId str isn't an int");
                        }
                    }
                case msgpack::type::NIL:
                    break;
                case msgpack::type::BOOLEAN:
                    sequency_id = msgpack_obj_index->via.boolean ? 1 : 0;
                    break;
                case msgpack::type::POSITIVE_INTEGER:
                case msgpack::type::NEGATIVE_INTEGER:
                    sequency_id = msgpack_obj_index->via.i64;
                    break;
                case msgpack::type::FLOAT32:
                case msgpack::type::FLOAT64:
                    sequency_id = static_cast<int64_t>(msgpack_obj_index->via.f64);
                    break;
                case msgpack::type::BIN:
                    throw signalr_exception("Can't parse sequence message's SequenceId from binary");
                    break;
                case msgpack::type::ARRAY:
                    throw signalr_exception("Can't convert sequence message's SequenceId from an array");
                    break;
                case msgpack::type::MAP:
                    throw signalr_exception("Can't convert sequence message's SequenceId from a map");
                    break;
                case msgpack::type::EXT:
                    throw signalr_exception("Can't convert sequence message's SequenceId from EXT type");
                    break;
                default:
                    throw signalr_exception(
                        "encountered unknown type while reading a sequency message's 'SequenceId' field: " +
                        std::to_string(msgpack_obj_index->type)
                    );
                }
                m_message_handler->handle_sequence_message(std::move(sequence_message(sequency_id)));
                break;
            }
            default:
                // Future protocol changes can add message types, old clients can ignore them
                break;
            }

            remaining_message += length_of_message;
            assert(remaining_message_length - length_of_message < remaining_message_length);
            remaining_message_length -= length_of_message;
        }
    }
}

#endif
