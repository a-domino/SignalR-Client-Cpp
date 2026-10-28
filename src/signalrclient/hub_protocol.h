// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "signalrclient/signalr_value.h"
#include "signalrclient/transfer_format.h"
#include "message_type.h"
#include <memory>
#include <utility>

namespace signalr
{
    template<signalr::message_type message_type>
    struct hub_message
    {
        virtual ~hub_message() = default;
        static const signalr::message_type msg_type = message_type;
    };

    template<signalr::message_type message_type>
    struct hub_invocation_message : hub_message<message_type>
    {
        explicit hub_invocation_message(const std::string& invocation_id) : invocation_id(invocation_id) { }

        explicit hub_invocation_message(std::string&& invocation_id) : invocation_id(std::move(invocation_id)) {}

        std::string invocation_id;
    };

    struct invocation_message : hub_invocation_message<signalr::message_type::invocation>
    {
        invocation_message(
            const std::string& invocation_id, const std::string& target, const std::vector<signalr::value>& args,
            const std::vector<std::string>& stream_ids = std::vector<std::string>()
        ) : hub_invocation_message(invocation_id), target(target), arguments(args), stream_ids(stream_ids)
        { }

        invocation_message(
            std::string&& invocation_id, std::string&& target, std::vector<signalr::value>&& args,
            std::vector<std::string>&& stream_ids = std::vector<std::string>()
        ) : hub_invocation_message(std::move(invocation_id))
          , target(target)
          , arguments(args)
          , stream_ids(stream_ids)
        { }

        std::string target;
        std::vector<signalr::value> arguments;
        std::vector<std::string> stream_ids;
    };

    struct stream_item_message : hub_invocation_message<signalr::message_type::stream_item> {
        stream_item_message(const std::string& invocation_id, const signalr::value& item)
            : hub_invocation_message(invocation_id), item(item) {}

        stream_item_message(std::string&& invocation_id, signalr::value&& item)
            : hub_invocation_message(std::move(invocation_id)), item(std::move(item)) {}

        signalr::value item;
    };

    struct completion_message : hub_invocation_message<signalr::message_type::completion>
    {
        enum class result_kind {
            error_result = 1,
            void_result = 2,
            non_void_result = 3
        };

        completion_message(
            const std::string& invocation_id, const std::string& error, const signalr::value& result,
            const result_kind result_kind
        ) : hub_invocation_message(invocation_id), error(error), result(result), kind_of_result(result_kind)
        { }

        completion_message(
            std::string&& invocation_id, std::string&& error, signalr::value&& result, const result_kind result_kind
        ) : hub_invocation_message(std::move(invocation_id)), error(error), result(result)
          , kind_of_result(result_kind)
        { }

        std::string error;
        signalr::value result;
        result_kind kind_of_result;
    };

    struct stream_invocation_message : hub_invocation_message<signalr::message_type::stream_invocation> {
        stream_invocation_message(
            const std::string& invocation_id, const std::string& target, const std::vector<signalr::value>& arguments,
            const std::vector<std::string>& stream_ids = std::vector<std::string>()
        ) : hub_invocation_message(invocation_id)
          , target(target)
          , arguments(arguments)
          , stream_ids(stream_ids){}

        stream_invocation_message(
            std::string&& invocation_id, std::string&& target, std::vector<signalr::value>&& arguments,
            std::vector<std::string>&& stream_ids = std::vector<std::string>()
        ) : hub_invocation_message(std::move(invocation_id))
          , target(std::move(target))
          , arguments(std::move(arguments))
          , stream_ids(std::move(stream_ids)){}

        std::string target;
        std::vector<signalr::value> arguments;
        std::vector<std::string> stream_ids;
    };

    struct cancel_invocation_message : hub_invocation_message<signalr::message_type::cancel_invocation> {
        cancel_invocation_message(const std::string& invocation_id) : hub_invocation_message(invocation_id) {}

        cancel_invocation_message(std::string&& invocation_id) : hub_invocation_message(std::move(invocation_id)) {}
    };

    struct ping_message : hub_message<signalr::message_type::ping> {
        ping_message() = default;
    };

    struct close_message : hub_message<signalr::message_type::close> {
        close_message(
            std::string&& error, const bool allow_reconnect = false
        ) : hub_message(), error(std::move(error)), allow_reconnect(allow_reconnect) {}

        std::string error;
        bool allow_reconnect;
    };

    struct ack_message : hub_message<signalr::message_type::ack> {
        ack_message(const int64_t sequence_id) : hub_message(), sequence_id(sequence_id) {}

        int64_t sequence_id;
    };

    // Only sent on reconnects.
    struct sequence_message : hub_message<signalr::message_type::sequence> {
        sequence_message(const int64_t sequence_id) : hub_message(), sequence_id(sequence_id) {}

        int64_t sequence_id;
    };

    class message_handler {
    public:
        virtual void handle_invocation_message(invocation_message&& invocation_message) = 0;
        virtual void handle_stream_item_message(stream_item_message&& stream_item_message) = 0;
        virtual void handle_completion_message(completion_message&& completion_message) = 0;
        virtual void handle_stream_invocation_message(stream_invocation_message&& stream_invocation_message) = 0;
        virtual void handle_cancel_invocation_message(cancel_invocation_message&& cancel_invocation_message) = 0;
        virtual void handle_ping_message(ping_message&& ping_message) = 0;
        virtual void handle_close_message(close_message&& close_message) = 0;
        virtual void handle_ack_message(ack_message&& ack_message) = 0;
        virtual void handle_sequence_message(sequence_message&& sequence_message) = 0;
        virtual ~message_handler() = default;
    };

    class hub_protocol
    {
    public:
        class parser_and_handler {
        public:
            virtual void parse_and_handle_messages(const std::string&) const = 0;
            virtual ~parser_and_handler() = default;
        };

        virtual std::string write_invocation_message(const invocation_message*) const = 0;
        virtual std::string write_stream_item_message(const stream_item_message*) const = 0;
        virtual std::string write_completion_message(const completion_message*) const = 0;
        virtual std::string write_stream_invocation_message(const stream_invocation_message*) const = 0;
        virtual std::string write_cancel_invocation_message(const cancel_invocation_message*) const = 0;
        virtual std::string write_ping_message(const ping_message*) const = 0;
        virtual std::string write_close_message(const close_message*) const = 0;
        virtual std::string write_ack_message(const ack_message*) const = 0;
        virtual std::string write_sequence_message(const sequence_message*) const = 0;
        virtual std::unique_ptr<parser_and_handler> create_parser_and_handler(const std::shared_ptr<message_handler>&) const = 0;
        virtual const std::string& name() const = 0;
        virtual int version() const = 0;
        virtual signalr::transfer_format transfer_format() const = 0;
        virtual ~hub_protocol() = default;
    };
}