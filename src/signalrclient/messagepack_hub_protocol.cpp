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

#include "signalrclient/trace_level.h"

namespace signalr
{
    // replaces use of msgpack::sbuffer when packing so that we can write directly to the string instead of writing to another buffer and copying to a string later
    class string_wrapper
    {
    public:
        std::string str;

        string_wrapper& write(const char* buf, size_t s)
        {
            str.append(buf, s);
            return *this;
        }
    };

    signalr::value createValue(const msgpack::object& v)
    {
        switch (v.type)
        {
        case msgpack::type::object_type::BOOLEAN:
            return signalr::value(v.via.boolean);
        case msgpack::type::object_type::FLOAT64:
        case msgpack::type::object_type::FLOAT32:
            return signalr::value(v.via.f64);
        case msgpack::type::object_type::POSITIVE_INTEGER:
            return signalr::value((double)v.via.u64);
        case msgpack::type::object_type::NEGATIVE_INTEGER:
            return signalr::value((double)v.via.i64);
        case msgpack::type::object_type::STR:
            return signalr::value(v.via.str.ptr, v.via.str.size);
        case msgpack::type::object_type::ARRAY:
        {
            std::vector<signalr::value> vec;
            for (size_t i = 0; i < v.via.array.size; ++i)
            {
                vec.push_back(createValue(*(v.via.array.ptr + i)));
            }
            return signalr::value(std::move(vec));
        }
        case msgpack::type::object_type::MAP:
        {
            std::map<std::string, signalr::value> map;
            for (size_t i = 0; i < v.via.map.size; ++i)
            {
                auto key = (v.via.map.ptr + i)->key.as<std::string>();
                map.insert({ key, createValue((v.via.map.ptr + i)->val) });
            }
            return signalr::value(std::move(map));
        }
        case msgpack::type::object_type::BIN:
        {
            std::vector<uint8_t> vec = std::vector<uint8_t>(v.via.bin.ptr, v.via.bin.ptr + v.via.bin.size);
            return signalr::value(std::move(vec));
        }
        case msgpack::type::object_type::EXT:
            throw signalr_exception("messagepack type 'EXT' not supported");
        case msgpack::type::object_type::NIL:
        default:
            return signalr::value();
        }
    }

    void pack_messagepack(const signalr::value& v, msgpack::packer<string_wrapper>& packer)
    {
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
            auto value = v.as_double();
            double intPart;
            // Workaround for 1.0 being output as 1.0 instead of 1
            // because the server expects certain values to be 1 instead of 1.0 (like protocol version)
            if (std::modf(value, &intPart) == 0)
            {
                if (value < 0)
                {
                    if (value >= (double)INT64_MIN)
                    {
                        // Fits within int64_t
                        packer.pack_int64(static_cast<int64_t>(intPart));
                        return;
                    }
                    else
                    {
                        // Remain as double
                        packer.pack_double(value);
                        return;
                    }
                }
                else
                {
                    if (value <= (double)UINT64_MAX)
                    {
                        // Fits within uint64_t
                        packer.pack_uint64(static_cast<uint64_t>(intPart));
                        return;
                    }
                    else
                    {
                        // Remain as double
                        packer.pack_double(value);
                        return;
                    }
                }
            }
            packer.pack_double(v.as_double());
            return;
        }
        case signalr::value_type::string:
        {
            auto length = v.as_string().length();
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
                pack_messagepack(val, packer);
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
                pack_messagepack(val.second, packer);
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
        {
            packer.pack_nil();
            return;
        }
        }
    }

    std::string signalr::messagepack_hub_protocol::write_message(const hub_message* hub_message) const
    {
        string_wrapper str;
        msgpack::packer<string_wrapper> packer(str);

#pragma warning (push)
#pragma warning (disable: 4061)
        switch (hub_message->message_type)
        {
        case message_type::invocation:
        {
            auto invocation = static_cast<invocation_message const*>(hub_message);

            packer.pack_array(6);

            packer.pack_int(static_cast<int>(message_type::invocation));
            // Headers
            packer.pack_map(0);

            if (invocation->invocation_id.empty())
            {
                packer.pack_nil();
            }
            else
            {
                packer.pack_str(static_cast<uint32_t>(invocation->invocation_id.length()));
                packer.pack_str_body(invocation->invocation_id.data(), static_cast<uint32_t>(invocation->invocation_id.length()));
            }

            packer.pack_str(static_cast<uint32_t>(invocation->target.length()));
            packer.pack_str_body(invocation->target.data(), static_cast<uint32_t>(invocation->target.length()));

            packer.pack_array(static_cast<uint32_t>(invocation->arguments.size()));
            for (auto& val : invocation->arguments)
            {
                pack_messagepack(val, packer);
            }

            // StreamIds
            packer.pack_array(0);

            break;
        }
        case message_type::completion:
        {
            auto completion = static_cast<completion_message const*>(hub_message);

            size_t result_kind = completion->error.empty() ? (completion->has_result ? 3U : 2U) : 1U;
            packer.pack_array(4U + (result_kind != 2U ? 1U : 0U));

            packer.pack_int(static_cast<int>(message_type::completion));

            // Headers
            packer.pack_map(0);

            packer.pack_str(static_cast<uint32_t>(completion->invocation_id.length()));
            packer.pack_str_body(completion->invocation_id.data(), static_cast<uint32_t>(completion->invocation_id.length()));

            packer.pack_int(static_cast<int>(result_kind));
            switch (result_kind)
            {
            // error result
            case 1:
                packer.pack_str(static_cast<uint32_t>(completion->error.length()));
                packer.pack_str_body(completion->error.data(), static_cast<uint32_t>(completion->error.length()));
                break;
            // non-void result
            case 3:
                pack_messagepack(completion->result, packer);
                break;
            }

            break;
        }
        case message_type::ping:
        {
            // If we need the ping this is how you get it
            // auto ping = static_cast<ping_message const*>(hub_message);

            packer.pack_array(1);
            packer.pack_int(static_cast<int>(message_type::ping));

            break;
        }
        default:
            break;
        }
#pragma warning (pop)

        binary_message_formatter::write_length_prefix(str.str);
        return str.str;
    }

    std::vector<std::unique_ptr<hub_message>> messagepack_hub_protocol::parse_messages(const std::string& message) const
    {
        std::vector<std::unique_ptr<hub_message>> vec;

        size_t length_prefix_length;
        size_t length_of_message;
        const char* remaining_message = message.data();
        size_t remaining_message_length = message.length();

        while (binary_message_parser::try_parse_message(reinterpret_cast<const unsigned char*>(remaining_message), remaining_message_length, &length_prefix_length, &length_of_message))
        {
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
            case message_type::stream_invocation:
            case message_type::invocation:
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

                if (type == message_type::invocation) {
                    vec.emplace_back(
                        std::unique_ptr<hub_message>(
                            new invocation_message(std::move(invocation_id), std::move(target), std::move(args))
                        )
                    );
                } else {
                    vec.emplace_back(
                        std::unique_ptr<hub_message>(
                            new stream_invocation_message(std::move(invocation_id), std::move(target), std::move(args))
                        )
                    );
                }

                if (num_elements_of_message > 5)
                {
                    // This is for the StreamIds when they are supported
                    // TODO: add StreamId support...
                    ++msgpack_obj_index;
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
                auto result_kind = msgpack_obj_index->via.i64;
                ++msgpack_obj_index;

                std::string error;
                signalr::value result;
                // 1: error
                // 2: void result
                // 3: non void result
                switch (result_kind) {
                case 1: // error
                    if (num_elements_of_message < 5) {
                        throw signalr_exception("completion error response has too few properties");
                    }
                    if (msgpack_obj_index->type != msgpack::type::STR)
                    {
                        throw signalr_exception("reading 'error' as string failed");
                    }
                    error.append(msgpack_obj_index->via.str.ptr, msgpack_obj_index->via.str.size);
                    break;
                case 2: // void
                    break;
                case 3: // non-void result
                    if (num_elements_of_message < 5) {
                        throw signalr_exception("completion result response has too few properties");
                    }
                    result = createValue(*msgpack_obj_index);
                    break;
                default:
                    throw signalr_exception("Unknown completion result type: " + std::to_string(result_kind));
                }

                vec.emplace_back(
                    std::unique_ptr<hub_message>(
                        new completion_message(
                            std::move(invocation_id), std::move(error), std::move(result), result_kind == 3
                        )
                    )
                );
                break;
            }
            case message_type::ping:
            {
                vec.emplace_back(std::unique_ptr<hub_message>(new ping_message()));
                break;
            }
            // TODO: other message types
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

                vec.emplace_back(
                    std::unique_ptr<hub_message>(
                        new stream_item_message(std::move(invocation_id), std::move(item))
                    )
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

                vec.emplace_back(
                    std::unique_ptr<hub_message>(new cancel_invocation_message(std::move(invocation_id)))
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

                vec.emplace_back(
                    std::unique_ptr<hub_message>(new close_message(std::move(error), allow_reconnect))
                );
                break;
            }
            case message_type::sequence:
            case message_type::ack: {
                // Ack messages have the following structure:
                //   -> [8, SequenceId]
                //
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
                            if (type == message_type::sequence) {
                                throw signalr_exception("sequence message's SequenceId str isn't an int");
                            }
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
                    if (type == message_type::sequence) {
                        throw signalr_exception("Can't parse sequence message's SequenceId from binary");
                    }
                    throw signalr_exception("Can't parse ack message's SequenceId str from binary");
                    break;
                case msgpack::type::ARRAY:
                    if (type == message_type::sequence) {
                        throw signalr_exception("Can't convert sequence message's SequenceId from an array");
                    }
                    throw signalr_exception("Can't convert ack message's SequenceId str from an array");
                    break;
                case msgpack::type::MAP:
                    if (type == message_type::sequence) {
                        throw signalr_exception("Can't convert sequence message's SequenceId from a map");
                    }
                    throw signalr_exception("Can't convert ack message's SequenceId str from a map");
                    break;
                case msgpack::type::EXT:
                    if (type == message_type::sequence) {
                        throw signalr_exception("Can't convert sequence message's SequenceId from EXT type");
                    }
                    throw signalr_exception("Can't convert ack message's SequenceId str from EXT type");
                    break;
                default:
                    if (type == message_type::sequence) {
                        throw signalr_exception(
                            "encountered unknown type while reading a sequency message's 'SequenceId' field: " +
                            std::to_string(msgpack_obj_index->type)
                        );
                    }
                    throw signalr_exception(
                        "encountered unknown type while reading an ack message's 'SequenceId' field: " +
                        std::to_string(msgpack_obj_index->type)
                    );
                }

                if (type == message_type::sequence) {
                    vec.emplace_back(
                        std::unique_ptr<hub_message>(new sequence_message(sequency_id))
                    );
                } else {
                    vec.emplace_back(
                        std::unique_ptr<hub_message>(new ack_message(sequency_id))
                    );
                }
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

        return vec;
    }
}

#endif
