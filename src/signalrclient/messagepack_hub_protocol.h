// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#ifdef USE_MSGPACK

#include "signalrclient/signalr_value.h"
#include "hub_protocol.h"

namespace signalr
{
    class messagepack_hub_protocol : public hub_protocol
    {
        class parser_and_handler : public hub_protocol::parser_and_handler {
        public:
            explicit parser_and_handler(const std::shared_ptr<message_handler>& msg_handler)
                    : m_message_handler(msg_handler) {}

            void parse_and_handle_messages(const std::string&) const override;

        private:
            const std::shared_ptr<message_handler> m_message_handler;
        };

    public:
        template<signalr::message_type message_type>
        std::string write_message(const hub_message<message_type>*) const;

        std::string write_invocation_message(const invocation_message*) const override;
        std::string write_stream_item_message(const stream_item_message*) const override;
        std::string write_completion_message(const completion_message*) const override;
        std::string write_stream_invocation_message(const stream_invocation_message*) const override;
        std::string write_cancel_invocation_message(const cancel_invocation_message*) const override;
        std::string write_ping_message(const ping_message*) const override;
        std::string write_close_message(const close_message*) const override;
        std::string write_ack_message(const ack_message*) const override;
        std::string write_sequence_message(const sequence_message*) const override;

        std::unique_ptr<hub_protocol::parser_and_handler> create_parser_and_handler(
            const std::shared_ptr<message_handler>& msg_handler
        ) const override {
            return std::unique_ptr<parser_and_handler>(new parser_and_handler{msg_handler});
        }

        const std::string& name() const override {
            return m_protocol_name;
        }

        int version() const override {
            return 1;
        }

        signalr::transfer_format transfer_format() const override {
            return signalr::transfer_format::binary;
        }

        ~messagepack_hub_protocol() override = default;

    private:
        static const std::string m_protocol_name;
    };
}

#endif