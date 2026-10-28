// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include <unordered_map>
#include <utility>
#include "hub_callback_manager.h"
#include "case_insensitive_comparison_utils.h"
#include "completion_event.h"
#include "signalrclient/signalr_value.h"
#include "hub_protocol.h"
#include "logger.h"
#include "cancellation_token_source.h"
#include "connection_impl.h"
#include "signalrclient/cancel_stream_interface.h"
#include "signalrclient/hub_connection.h"

namespace signalr
{
    class websocket_client;

    // Note:
    // Factory methods and private constructors prevent from using this class incorrectly. Because this class
    // derives from `std::enable_shared_from_this` the instance has to be owned by a `std::shared_ptr` whenever
    // a member method calls `std::shared_from_this()` otherwise the behavior is undefined. Therefore constructors
    // are private to disallow creating instances directly and factory methods return `std::shared_ptr<connection_impl>`.
    class hub_connection_impl : public std::enable_shared_from_this<hub_connection_impl>
    {
    public:
        static std::shared_ptr<hub_connection_impl> create(
            const std::string& url, std::unique_ptr<hub_protocol>&& hub_protocol, trace_level trace_level,
            const std::shared_ptr<log_writer>& log_writer, const log_entry_handler& log_entry_handler,
            const std::function<std::shared_ptr<http_client>(const signalr_client_config&)>& http_client_factory,
            const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_factory,
            bool skip_negotiation = false
        );

        hub_connection_impl(const hub_connection_impl&) = delete;
        hub_connection_impl& operator=(const hub_connection_impl&) = delete;

        virtual ~hub_connection_impl();

        void on(const std::string& event_name, const method_invoked_handler& handler);

        void invoke(
            const std::string& method_name, const std::vector<value>& arguments,
            const std::function<void(value&&, const std::exception_ptr&)>& callback
        ) noexcept;
        void send(
            const std::string& method_name, const std::vector<signalr::value>& arguments,
            const std::function<void(std::exception_ptr)>& callback
        ) noexcept;

        void stream(
            const std::string& method_name, const std::vector<signalr::value>& arguments,
            const std::function<void(std::unique_ptr<cancel_stream_interface>)>& stream_invocation_sent_callback,
            const std::function<void(signalr::value&&)>& handle_item_callback,
            const std::function<void(value&&, const std::exception_ptr&)>& completion_callback
        );

        void start(const std::function<void(std::exception_ptr)>& callback) noexcept;
        void stop(
            const std::function<void(const std::exception_ptr&)>& callback,
            const std::exception_ptr& reason = nullptr, bool is_dtor = false
        ) noexcept;

        connection_state get_connection_state() const noexcept;
        std::string get_connection_id() const;

        void set_client_config(const signalr_client_config& config);
        void set_disconnected(const std::function<void(std::exception_ptr)>& disconnected);

    private:
        hub_connection_impl(
            const std::string& url, std::unique_ptr<hub_protocol>&& hub_protocol, trace_level trace_level,
            const std::shared_ptr<log_writer>& log_writer, const log_entry_handler& log_entry_handler,
            const std::function<std::shared_ptr<http_client>(const signalr_client_config&)>& http_client_factory,
            const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_factory,
            bool skip_negotiation
        );

        class same_thread_message_handler : public std::enable_shared_from_this<same_thread_message_handler>
                                          , public message_handler {
        public:
            static std::shared_ptr<same_thread_message_handler> create(
                const std::weak_ptr<hub_connection_impl>& hub_conn_impl
            ) noexcept {
                return std::shared_ptr<same_thread_message_handler>(new same_thread_message_handler(hub_conn_impl));
            }

            void handle_invocation_message(invocation_message&& invocation_msg) noexcept override;
            void handle_stream_item_message(stream_item_message&& stream_item_message) noexcept override;
            void handle_completion_message(completion_message&& completion_message) noexcept override;
            void handle_stream_invocation_message(stream_invocation_message&& stream_invocation_message) noexcept override;
            void handle_cancel_invocation_message(cancel_invocation_message&& cancel_invocation_message) noexcept override;
            void handle_ping_message(ping_message&& ping_message) noexcept override;
            void handle_close_message(close_message&& close_message) noexcept override;
            void handle_ack_message(ack_message&& ack_message) noexcept override;
            void handle_sequence_message(sequence_message&& sequence_message) noexcept override;

        private:
            explicit same_thread_message_handler(
                const std::weak_ptr<hub_connection_impl>& hub_conn_impl
            ) noexcept : m_hub_conn_impl(hub_conn_impl) {};

            std::weak_ptr<hub_connection_impl> m_hub_conn_impl;
        };

        class cancel_stream : public cancel_stream_interface {
        public:
            cancel_stream(
                const std::weak_ptr<hub_connection_impl>& hub, std::string callback_id
            ) : m_hub(hub), m_invocation_id(std::move(callback_id))
            {}

            void cancel(const std::function<void(const std::exception_ptr&)>& callback) override;

            ~cancel_stream() override;

        private:
            std::weak_ptr<hub_connection_impl> m_hub;
            const std::string m_invocation_id;
        };

        std::shared_ptr<connection_impl> m_connection;
        logger m_logger;
        hub_callback_manager m_callback_manager;
        mutable std::mutex m_subscriptions_mutex;
        std::unordered_map<std::string, method_invoked_handler, case_insensitive_hash, case_insensitive_equals> m_subscriptions;
        bool m_handshakeReceived;
        std::shared_ptr<completion_event> m_handshakeTask;
        std::function<void(const std::exception_ptr&)> m_disconnected;
        std::shared_ptr<cancellation_token_source> m_disconnect_cts;
        signalr_client_config m_signalr_client_config;
        std::shared_ptr<message_handler> m_message_handler;
        std::unique_ptr<hub_protocol> m_protocol;
        std::unique_ptr<hub_protocol::parser_and_handler> m_parser_and_handler;
        std::string m_cached_ping;

        std::atomic<int64_t> m_nextActivationServerTimeout{};
        std::atomic<int64_t> m_nextActivationSendPing{};

        std::mutex m_stop_callback_lock;
        std::vector<std::function<void(std::exception_ptr)>> m_stop_callbacks;

        void initialize();
        void stop_connection(const std::exception_ptr& reason = nullptr) noexcept;

        void process_response(std::string&& response);

        bool invoke_callback(completion_message* completion);
        void handle_invoke_request(const invocation_message* invocation) const;
        void send_invoke_completion(const std::string& invocation_id, const signalr::value& response, const std::exception_ptr& err) const;

        void reset_send_ping();
        void reset_server_timeout();

        void start_keepalive();

        void send_cancel_stream(
            const std::string& invocation_id, const std::function<void(const std::exception_ptr&)>& callback
        ) const;
    };
}
