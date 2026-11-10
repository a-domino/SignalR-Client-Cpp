// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "json_hub_protocol.h"

#include <cmath>

#include "message_type.h"
#include "json_helpers.h"
#include "signalrclient/signalr_exception.h"

namespace signalr
{
const std::string json_hub_protocol::m_protocol_name = "json";

void json_hub_protocol::parser_and_handler::parse_and_handle_messages(const std::string& msg) const {
    size_t offset = 0;
    for (auto pos = msg.find(JSON_MSG_SEP, offset); pos != std::string::npos; pos = msg.find(JSON_MSG_SEP, offset)) {
        parse_and_handle_message(msg.c_str() + offset, pos - offset);
        offset = pos + 1;
    }
    if (offset < msg.size()) {
        throw signalr_exception(
            "The end of the last parsed message wasn't parsed "
            "(i.e., offset into string message, " + std::to_string(offset) +
            ", < string message size, " + std::to_string(msg.size()) + ")"
        );
    }
}

void json_hub_protocol::parser_and_handler::parse_and_handle_message(const char* begin, size_t length) const {
    Json::Value root;
    const auto reader = getJsonReader();
    std::string errors;

    if (!reader->parse(begin, begin + length, &root, &errors))
    {
        throw signalr_exception(errors);
    }

    if (root.type() != Json::ValueType::objectValue) {
        throw signalr_exception("Message was not a 'map' type");
    }

    const auto type = root.find("type");
    if (type == nullptr) {
        throw signalr_exception("Field 'type' not found");
    }
    message_type msg_type;
    switch (type->type()) {
    case Json::nullValue:
        throw signalr_exception("Field 'type' not found");
    case Json::intValue:
        msg_type = static_cast<message_type>(type->asInt());
        break;
    case Json::uintValue:
        msg_type = static_cast<message_type>(type->asUInt());
        break;
    case Json::realValue: {
        const auto dbl_val = type->asDouble();
        if (std::floor(dbl_val) == dbl_val) {
            msg_type = static_cast<message_type>(dbl_val);
        } else {
            throw signalr_exception(
                "'type' field enum does not store an integer value: " + std::to_string(dbl_val)
            );
        }
        break;
    }
    case Json::stringValue: {
        size_t pos;
        const auto type_str = type->asString();
        try {
            const auto type_int_val = std::stoi(type_str, &pos);
            if (pos == type_str.size()) {
                msg_type = static_cast<message_type>(type_int_val);
                break;
            }
        } catch (...) { }
        throw signalr_exception("'type' field enum does not store an integer value: '" + type_str + "'");
    }
    case Json::booleanValue:
        msg_type = static_cast<message_type>(type->asBool() ? 1 : 0);
        break;
    case Json::arrayValue:
    case Json::objectValue:
        throw signalr_exception("'type' field enum does not store an integer value: " + type->toStyledString());
    default:
        throw signalr_exception("Unknown JSON Type for 'type': " + std::to_string(static_cast<int>(type->type())));
    }

    switch (msg_type) {
    case message_type::invocation: {
        const auto target = root.find("target");
        if (target == nullptr) {
            throw signalr_exception("Field 'target' not found for 'invocation' message");
        }
        if (!target->isString())
        {
            throw signalr_exception("Expected 'invocation' message's 'target' to be of type 'string'");
        }

        const auto args = root.find("arguments");
        if (args == nullptr)
        {
            throw signalr_exception("Field 'arguments' not found for 'invocation' message");
        }
        if (!args->isArray())
        {
            throw signalr_exception("Expected 'invocation' message's 'arguments' to be of type 'array'");
        }
        std::vector<value> args_vector;
        for (const auto& arg : *args) {
            args_vector.push_back(createValue(arg));
        }

        const auto invocation_id = root.find("invocationId");
        if (invocation_id == nullptr)
        {
            m_message_handler->handle_invocation_message(
                invocation_message{"", target->asString(), std::move(args_vector)}
            );
        }
        else
        {
            if (!invocation_id->isString())
            {
                throw signalr_exception("Expected 'invocation' message's 'invocationId' to be of type 'string'");
            }

            m_message_handler->handle_invocation_message(
                invocation_message{invocation_id->asString(), target->asString(), std::move(args_vector)}
            );
        }
        break;
    }
    case message_type::stream_item: {
        const auto item = root.find("item");
        if (item == nullptr) {
            throw signalr_exception("Field 'item' not found in 'stream_item' message");
        }

        const auto invocation_id = root.find("invocationId");
        if (invocation_id == nullptr) {
            throw signalr_exception("Field 'invocationId' not found in 'stream_item' message");
        }
        if (!invocation_id->isString()) {
            throw signalr_exception("Expected 'stream_item' message's 'invocationId' to be of type 'string'");
        }

        m_message_handler->handle_stream_item_message(
            stream_item_message{invocation_id->asString(), createValue(*item)}
        );
        break;
    }
    case message_type::completion: {
        const auto invocation_id = root.find("invocationId");
        if (invocation_id == nullptr) {
            throw signalr_exception("Field 'invocationId' not found for 'completion' message");
        }
        if (!invocation_id->isString()) {
            throw signalr_exception("Expected 'completion' message's 'invocationId' to be of type 'string'");
        }

        const auto result = root.find("result");
        const auto error = root.find("error");

        if (error != nullptr) {
            if (!error->isString()) {
                throw signalr_exception("Expected 'completion' message's 'error' field to be of type 'string'");
            }
            if (result != nullptr) {
                throw signalr_exception("Found both 'error' and 'result' fields in 'completion' message. "
                                        "The 'error' and 'result' properties are mutually exclusive.");
            }
            m_message_handler->handle_completion_message(
                completion_message(
                    invocation_id->asString(), error->asString(), signalr::value(),
                    completion_message::result_kind::error_result
                )
            );
        } else if (result == nullptr) {
            m_message_handler->handle_completion_message(
                completion_message(
                    invocation_id->asString(), "", signalr::value(),
                    completion_message::result_kind::void_result
                )
            );
        } else {
            m_message_handler->handle_completion_message(
                completion_message(
                    invocation_id->asString(), "", createValue(*result),
                    completion_message::result_kind::non_void_result
                )
            );
        }

        break;
    }
    case message_type::stream_invocation: {
        const auto target = root.find("target");
        if (target == nullptr) {
            throw signalr_exception("Field 'target' not found for 'stream_invocation' message");
        }
        if (!target->isString())
        {
            throw signalr_exception("Expected 'stream_invocation' message's 'target' to be of type 'string'");
        }

        const auto args = root.find("arguments");
        if (args == nullptr)
        {
            throw signalr_exception("Field 'arguments' not found for 'stream_invocation' message");
        }
        if (!args->isArray())
        {
            throw signalr_exception("Expected 'stream_invocation' message's 'arguments' to be of type 'array'");
        }
        std::vector<value> args_vector;
        for (const auto& arg : *args) {
            args_vector.push_back(createValue(arg));
        }

        const auto invocation_id = root.find("invocationId");
        if (invocation_id == nullptr)
        {
            m_message_handler->handle_stream_invocation_message(
                stream_invocation_message("", target->asString(), std::move(args_vector))
            );
        }
        else
        {
            if (!invocation_id->isString())
            {
                throw signalr_exception(
                    "Expected 'stream_invocation' message's 'invocationId' to be of type 'string'"
                );
            }

            m_message_handler->handle_stream_invocation_message(
                stream_invocation_message(invocation_id->asString(), target->asString(), std::move(args_vector))
            );
        }

        break;
    }
    case message_type::cancel_invocation: {
        const auto invocation_id = root.find("invocationId");
        if (invocation_id == nullptr) {
            throw signalr_exception("Field 'invocationId' not found for 'cancel_invocation' message");
        }
        if (!invocation_id->isString()) {
            throw signalr_exception("Expected 'cancel_invocation' message's 'invocationId' to be of type 'string'");
        }

        m_message_handler->handle_cancel_invocation_message(cancel_invocation_message(invocation_id->asString()));
        break;
    }
    case message_type::ping:
        m_message_handler->handle_ping_message(ping_message());
        break;
    case message_type::close: {
        const auto error = root.find("error");
        const auto allow_reconnect = root.find("allowReconnect");

        m_message_handler->handle_close_message(
            close_message(
                error == nullptr || !error->isString() ? "" : error->asString(),
                allow_reconnect == nullptr || !allow_reconnect->isBool() ? false : allow_reconnect->asBool()
            )
        );
        break;
    }
    case message_type::ack: {
        const auto sequence_id = root.find("sequenceId");
        if (sequence_id == nullptr) {
            throw signalr_exception("Field 'sequenceId' not found in 'ack' message");
        }
        int64_t seq_id;
        switch (sequence_id->type()) {
        case Json::intValue:
        case Json::uintValue:
            seq_id = sequence_id->asInt64();
            break;
        case Json::realValue: {
            const auto dbl_seq_id = sequence_id->asDouble();
            if (std::floor(dbl_seq_id) == dbl_seq_id) {
                seq_id = sequence_id->asInt64();
                break;
            }
        }
        /* fall through */
        default:
            throw signalr_exception("Expected 'sequenceId' in 'ack' message to be an Int");
        }

        m_message_handler->handle_ack_message(ack_message(seq_id));
        break;
    }
    case message_type::sequence: {
        const auto sequence_id = root.find("sequenceId");
        if (sequence_id == nullptr) {
            throw signalr_exception("Field 'sequenceId' not found in 'sequence' message");
        }
        int64_t seq_id;
        switch (sequence_id->type()) {
        case Json::intValue:
        case Json::uintValue:
            seq_id = sequence_id->asInt64();
            break;
        case Json::realValue: {
            const auto dbl_seq_id = sequence_id->asDouble();
            if (std::floor(dbl_seq_id) == dbl_seq_id) {
                seq_id = sequence_id->asInt64();
                break;
            }
        }
        /* fall through */
        default:
            throw signalr_exception("Expected 'sequenceId' in 'sequence' message to be an Int");
        }

        m_message_handler->handle_sequence_message(sequence_message(seq_id));
        break;
    }
    default:
        // Future protocol changes can add message types, old clients can ignore them
        break;
    }
}

std::string json_hub_protocol::write_invocation_message(const invocation_message* invocation) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::invocation);
    if (!invocation->invocation_id.empty())
    {
        object["invocationId"] = invocation->invocation_id;
    }
    object["target"] = invocation->target;
    object["arguments"] = createJson(invocation->arguments);
    // TODO: streamIds

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_stream_item_message(const stream_item_message* stream_item) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::stream_item);
    object["invocationId"] = stream_item->invocation_id;
    object["item"] = createJson(stream_item->item);

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_completion_message(const completion_message* completion) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::completion);
    object["invocationId"] = completion->invocation_id;
    switch (completion->kind_of_result) {
    case completion_message::result_kind::error_result:
        object["error"] = completion->error;
        break;
    case completion_message::result_kind::non_void_result:
        object["result"] = createJson(completion->result);
        break;
    case completion_message::result_kind::void_result:
    default:
        break;
    }

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_stream_invocation_message(
    const stream_invocation_message* stream_invocation
) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::stream_invocation);
    if (!stream_invocation->invocation_id.empty())
    {
        object["invocationId"] = stream_invocation->invocation_id;
    }
    object["target"] = stream_invocation->target;
    object["arguments"] = createJson(stream_invocation->arguments);
    // TODO: streamIds

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_cancel_invocation_message(
    const cancel_invocation_message* cancel_invocation
) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::cancel_invocation);
    object["invocationId"] = cancel_invocation->invocation_id;

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_ping_message(const ping_message*) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::ping);

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_close_message(const close_message* close) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::close);
    if (!close->error.empty()) {
        object["error"] = close->error;
    }
    object["allowReconnect"] = close->allow_reconnect;

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_ack_message(const ack_message* ack) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::ack);
    object["sequenceId"] = ack->sequence_id;

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

std::string json_hub_protocol::write_sequence_message(const sequence_message* sequence) const {
    Json::Value object(Json::ValueType::objectValue);

    object["type"] = static_cast<int>(message_type::sequence);
    object["sequenceId"] = sequence->sequence_id;

    return Json::writeString(getJsonWriter(), object) + JSON_MSG_SEP;
}

} // namespace signalr