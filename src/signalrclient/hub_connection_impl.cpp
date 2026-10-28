// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "hub_connection_impl.h"

#include <sstream>
#include <utility>
#include "signalrclient/hub_exception.h"
#include "trace_log_writer.h"
#include "signalrclient/signalr_exception.h"
#include "json_hub_protocol.h"
#include "message_type.h"
#include "handshake_protocol.h"
#include "signalrclient/websocket_client.h"
#include "signalr_default_scheduler.h"
#include "signalrclient/hub_connection.h"

namespace signalr
{
    // unnamed namespace makes it invisble outside this translation unit
    namespace
    {
        std::function<void(const char* error, value&&)> create_hub_invocation_callback(
            const std::function<void(value&&, std::exception_ptr)>& set_result
        );
    }

    std::shared_ptr<hub_connection_impl> hub_connection_impl::create(
        const std::string& url, std::unique_ptr<hub_protocol>&& hub_protocol, const trace_level trace_level,
        const std::shared_ptr<log_writer>& log_writer, const log_entry_handler& log_entry_handler,
        const std::function<std::shared_ptr<http_client>(const signalr_client_config&)>& http_client_factory,
        const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_factory,
        const bool skip_negotiation
    )
    {
        auto connection = std::shared_ptr<hub_connection_impl>(
            new hub_connection_impl(
                url, std::move(hub_protocol), trace_level, log_writer, log_entry_handler,
                http_client_factory, websocket_factory, skip_negotiation
            )
        );

        connection->initialize();

        return connection;
    }

    hub_connection_impl::hub_connection_impl(
        const std::string& url, std::unique_ptr<hub_protocol>&& hub_protocol, const trace_level trace_level,
        const std::shared_ptr<log_writer>& log_writer, const log_entry_handler& log_entry_handler,
        const std::function<std::shared_ptr<http_client>(const signalr_client_config&)>& http_client_factory,
        const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_factory,
        const bool skip_negotiation
    ) : m_connection(
            connection_impl::create(
                url, trace_level, log_writer, log_entry_handler, http_client_factory,
                websocket_factory, skip_negotiation
            )
        )
      , m_logger(log_writer, trace_level, log_entry_handler)
      , m_callback_manager("connection went out of scope before invocation result was received")
      , m_handshakeReceived(false), m_disconnected([](std::exception_ptr) noexcept {})
      , m_protocol(std::move(hub_protocol))
    {
        const ping_message ping_msg{};
        m_cached_ping = m_protocol->write_ping_message(&ping_msg);
    }

    void hub_connection_impl::same_thread_message_handler::handle_invocation_message(
        invocation_message&& invocation_msg
    ) {
        auto hub_conn_impl = m_hub_conn_impl.lock();
        if (hub_conn_impl == nullptr) {
            return;
        }
        method_invoked_handler event_handler = nullptr;
        {
            std::lock_guard<std::mutex> lock(hub_conn_impl->m_subscriptions_mutex);
            auto event = hub_conn_impl->m_subscriptions.find(invocation_msg.target);
            if (event != hub_conn_impl->m_subscriptions.end())
            {
                event_handler = event->second;
            }
        }
        if (event_handler != nullptr) {
            const auto& args = invocation_msg.arguments;
            signalr::value event_resp;
            try {
                event_resp = event_handler(args);
            }
            catch (const std::exception& e)
            {
                // Send completion message w/ invocation ID & error message.
                hub_conn_impl->send_invoke_completion(
                    invocation_msg.invocation_id, value(), std::make_exception_ptr(e)
                );
                return;
            }

            try {
                hub_conn_impl->send_invoke_completion(invocation_msg.invocation_id, event_resp, nullptr);
            }
            catch (const std::exception& e) {
                if (hub_conn_impl->m_logger.is_enabled(trace_level::error)) {
                    std::stringstream ss;
                    ss << "Encountered error while sending invoke completion response, '" << event_resp
                       << "', w/ invocationID=" << invocation_msg.invocation_id << ": " << e.what();
                    hub_conn_impl->m_logger.log(trace_level::error, ss.str());
                }
            }
            catch (...) {
                if (hub_conn_impl->m_logger.is_enabled(trace_level::error)) {
                    std::stringstream ss;
                    ss << "Encountered unknown error while sending invoke completion response, '"
                       << event_resp << "', w/ invocationID=" << invocation_msg.invocation_id << ": ";
                    hub_conn_impl->m_logger.log(trace_level::error, ss.str());
                }
            }
        } else {
            hub_conn_impl->m_logger.log(trace_level::info, "handler not found");
            hub_conn_impl->send_invoke_completion(
                invocation_msg.invocation_id, value(),
                std::make_exception_ptr(signalr_exception("handler not found"))
            );
        }
    }

    void hub_connection_impl::same_thread_message_handler::handle_stream_item_message(
        stream_item_message&& stream_item_msg
    ) {

    }

    bool hub_connection_impl::cancel_stream::cancel() {
        auto hub_conn_impl = m_hub.lock();
        if (hub_conn_impl == nullptr) {
            return false;
        }

        cancel_invocation_message cancel_msg{m_callback_id};


        return false;
    }

    void hub_connection_impl::initialize()
    {
        // weak_ptr prevents a circular dependency leading to memory leak and other problems
        std::weak_ptr<hub_connection_impl> weak_hub_connection = shared_from_this();

        m_message_handler = same_thread_message_handler::create(weak_hub_connection);
        m_parser_and_handler = m_protocol->create_parser_and_handler(m_message_handler);

        m_connection->set_message_received([weak_hub_connection](std::string&& message)
        {
            auto connection = weak_hub_connection.lock();
            if (connection)
            {
                connection->process_response(std::move(message));
            }
        });

        m_connection->set_disconnected(
            [weak_hub_connection](std::exception_ptr exception)
            {
                const auto connection = weak_hub_connection.lock();
                if (connection == nullptr) {
                    return;
                }
                // start may be waiting on the handshake response so we complete it here, this no-ops if already set
                connection->m_handshakeTask->set(std::make_exception_ptr(signalr_exception("connection closed while handshake was in progress.")));
                try
                {
                    connection->m_disconnect_cts->cancel();
                }
                catch (const std::exception& ex)
                {
                    if (connection->m_logger.is_enabled(trace_level::warning))
                    {
                        connection->m_logger.log(
                            trace_level::warning,
                            std::string("disconnect event threw an exception during connection closure: ") + ex.what()
                        );
                    }
                }

                connection->m_callback_manager.clear("connection was stopped before invocation result was received");

                connection->m_disconnected(std::move(exception));
            }
        );
    }

    void hub_connection_impl::on(const std::string& event_name, const method_invoked_handler& handler)
    {
        if (event_name.empty())
        {
            throw std::invalid_argument("event_name cannot be empty");
        }

        auto weak_connection = std::weak_ptr<hub_connection_impl>(shared_from_this());
        auto connection = weak_connection.lock();
        if (connection && connection->get_connection_state() != connection_state::disconnected)
        {
            throw signalr_exception("can't register a handler if the connection is not in a disconnected state");
        }

        std::lock_guard<std::mutex> lock(m_subscriptions_mutex);
        if (m_subscriptions.find(event_name) != m_subscriptions.end())
        {
            throw signalr_exception(
                "an action for this event has already been registered. event name: " + event_name);
        }

        m_subscriptions.insert({event_name, handler});
    }

    void hub_connection_impl::start(std::function<void(std::exception_ptr)> callback) noexcept
    {
        if (m_connection->get_connection_state() != connection_state::disconnected)
        {
            callback(std::make_exception_ptr(signalr_exception(
                "the connection can only be started if it is in the disconnected state")));
            return;
        }

        m_connection->set_client_config(m_signalr_client_config);
        m_handshakeTask = std::make_shared<completion_event>();
        m_disconnect_cts = std::make_shared<cancellation_token_source>();
        m_handshakeReceived = false;
        std::weak_ptr<hub_connection_impl> weak_connection = shared_from_this();
        m_connection->start([weak_connection, callback](std::exception_ptr start_exception)
            {
                auto connection = weak_connection.lock();
                if (!connection)
                {
                    // The connection has been destructed
                    callback(std::make_exception_ptr(signalr_exception("the hub connection has been deconstructed")));
                    return;
                }

                if (start_exception)
                {
                    assert(connection->get_connection_state() == connection_state::disconnected);
                    // connection didn't start, don't call stop
                    callback(start_exception);
                    return;
                }

                std::shared_ptr<std::mutex> handshake_request_lock = std::make_shared<std::mutex>();
                std::shared_ptr<bool> handshake_request_done = std::make_shared<bool>();

                auto handle_handshake = [weak_connection, handshake_request_done, handshake_request_lock, callback](std::exception_ptr exception, bool fromSend)
                {
                    assert(fromSend ? *handshake_request_done : true);

                    auto connection = weak_connection.lock();
                    if (!connection)
                    {
                        // The connection has been destructed
                        callback(std::make_exception_ptr(signalr_exception("the hub connection has been deconstructed")));
                        return;
                    }

                    {
                        std::lock_guard<std::mutex> lock(*handshake_request_lock);
                        // connection.send will be waiting on the handshake task which has been set by the caller already
                        if (!fromSend && *handshake_request_done == true)
                        {
                            return;
                        }
                        *handshake_request_done = true;
                    }

                    try
                    {
                        if (exception == nullptr)
                        {
                            connection->m_handshakeTask->get();
                            callback(nullptr);
                        }
                    }
                    catch (...)
                    {
                        exception = std::current_exception();
                    }

                    if (exception != nullptr)
                    {
                        connection->m_connection->stop([callback, exception](std::exception_ptr)
                            {
                                callback(exception);
                            }, exception);
                    }
                    else
                    {
                        connection->start_keepalive();
                    }
                };

                auto handshake_request = handshake::write_handshake(connection->m_protocol);
                auto handshake_task = connection->m_handshakeTask;
                auto handshake_timeout = connection->m_signalr_client_config.get_handshake_timeout();

                connection->m_disconnect_cts->register_callback(
                    [handle_handshake, handshake_request_lock, handshake_request_done]()
                    {
                        {
                            std::lock_guard<std::mutex> lock(*handshake_request_lock);
                            // no op after connection.send returned, m_handshakeTask should be set before
                            // m_disconnect_cts is canceled
                            if (*handshake_request_done == true)
                            {
                                return;
                            }
                        }

                        // if the request isn't completed then no one is waiting on the handshake task
                        // so we need to run the callback here instead of relying on connection.send completing
                        // handshake_request_done is set in handle_handshake, don't set it here
                        handle_handshake(nullptr, false);
                    });

                timer(connection->m_signalr_client_config.get_scheduler(),
                    [handle_handshake, handshake_task, handshake_timeout, handshake_request_lock](std::chrono::milliseconds duration)
                    {
                        {
                            std::lock_guard<std::mutex> lock(*handshake_request_lock);

                            // if the task is set then connection.send is either already waiting on the handshake or has completed,
                            // or stop has been called and will be handling the callback
                            if (handshake_task->is_set())
                            {
                                return true;
                            }

                            if (duration < handshake_timeout)
                            {
                                return false;
                            }
                        }

                        auto exception = std::make_exception_ptr(signalr_exception("timed out waiting for the server to respond to the handshake message."));
                        // unblocks connection.send if it's waiting on the task
                        handshake_task->set(exception);

                        handle_handshake(exception, false);
                        return true;
                    });

                connection->m_connection->send(handshake_request, connection->m_protocol->transfer_format(),
                                               [handle_handshake, handshake_request_done, handshake_request_lock](std::exception_ptr exception)
                                               {
                                                   {
                                                       std::lock_guard<std::mutex> lock(*handshake_request_lock);
                                                       if (*handshake_request_done == true)
                                                       {
                                                           // callback ran from timer or cancellation token, nothing to do here
                                                           return;
                                                       }

                                                       // indicates that the handshake timer doesn't need to call the callback, it just needs to set the timeout exception
                                                       // handle_handshake will be waiting on the handshake completion (error or success) to call the callback
                                                       *handshake_request_done = true;
                                                   }

                                                   handle_handshake(std::move(exception), true);
                                               });
            });
    }

    void hub_connection_impl::stop(std::function<void(std::exception_ptr)> callback, bool is_dtor) noexcept
    {
        if (get_connection_state() == connection_state::disconnected)
        {
            // don't log if already disconnected and stop called from dtor, it's just noise
            if (!is_dtor)
            {
                m_logger.log(trace_level::debug, "stop ignored because the connection is already disconnected.");
            }
            callback(nullptr);
            return;
        }
        else
        {
            {
                std::lock_guard<std::mutex> lock(m_stop_callback_lock);
                m_stop_callbacks.push_back(callback);

                if (m_stop_callbacks.size() > 1)
                {
                    m_logger.log(trace_level::info, "Stop is already in progress, waiting for it to finish.");
                    // we already registered the callback
                    // so we can just return now as the in-progress stop will trigger the callback when it completes
                    return;
                }
            }
            std::weak_ptr<hub_connection_impl> weak_connection = shared_from_this();
            m_connection->stop([weak_connection](std::exception_ptr exception)
                {
                    auto connection = weak_connection.lock();
                    if (!connection)
                    {
                        return;
                    }

                    assert(connection->get_connection_state() == connection_state::disconnected);

                    std::vector<std::function<void(std::exception_ptr)>> callbacks;

                    {
                        std::lock_guard<std::mutex> lock(connection->m_stop_callback_lock);
                        // copy the callbacks out and clear the list inside the lock
                        // then run the callbacks outside of the lock
                        callbacks = connection->m_stop_callbacks;
                        connection->m_stop_callbacks.clear();
                    }

                    for (auto& callback : callbacks)
                    {
                        callback(exception);
                    }
                }, nullptr);
        }
    }

    void hub_connection_impl::process_response(std::string&& response)
    {
        try
        {
            if (!m_handshakeReceived)
            {
                signalr::value handshake;
                std::tie(response, handshake) = handshake::parse_handshake(response);

                auto& obj = handshake.as_map();
                auto found = obj.find("error");
                if (found != obj.end())
                {
                    auto& error = found->second.as_string();
                    if (m_logger.is_enabled(trace_level::error))
                    {
                        m_logger.log(trace_level::error, std::string("handshake error: ").append(error));
                    }
                    m_handshakeTask->set(
                        std::make_exception_ptr(
                            signalr_exception(std::string("Received an error during handshake: ").append(error))
                        )
                    );
                    return;
                }
                found = obj.find("type");
                if (found != obj.end())
                {
                    m_handshakeTask->set(
                        std::make_exception_ptr(
                            signalr_exception("Received unexpected message while waiting for the handshake response.")
                        )
                    );
                    return;
                }

                m_handshakeReceived = true;
                m_handshakeTask->set();

                if (response.empty())
                {
                    return;
                }
            }

            reset_server_timeout();
            m_parser_and_handler->parse_and_handle_messages(response);

            auto messages = m_protocol->parse_and_handle_messages(response);

            for (const auto& val : messages)
            {
                // Protocol received an unknown message type and gave us a null object, close the connection like we do in other client implementations
                if (val == nullptr)
                {
                    throw std::runtime_error("null message received");
                }

                switch (val->message_type)
                {
                case message_type::invocation:
                {
                    auto invocation = static_cast<invocation_message*>(val.get());
                    handle_invoke_request(invocation);
                    break;
                }
                case message_type::stream_invocation:

                    // Sent to server only, should not be received by client
                    throw std::runtime_error("Received unexpected message type 'StreamInvocation'");
                case message_type::stream_item:
                    // TODO
                    break;
                case message_type::completion:
                {
                    auto completion = static_cast<completion_message*>(val.get());
                    invoke_callback(completion);
                    break;
                }
                case message_type::cancel_invocation:
                    // Sent to server only, should not be received by client
                    throw std::runtime_error("Received unexpected message type 'CancelInvocation'.");
                case message_type::ping:
                    if (m_logger.is_enabled(trace_level::debug))
                    {
                        m_logger.log(trace_level::debug, "ping message received.");
                    }
                    break;
                case message_type::close:
                    // TODO
                    break;
                default:
                    throw std::runtime_error(
                        "unknown message type '" + std::to_string(static_cast<int>(val->message_type)) + "' received"
                    );
                    break;
                }
            }
        }
        catch (const std::exception &e)
        {
            if (m_logger.is_enabled(trace_level::error))
            {
                m_logger.logv(
                    trace_level::error, "error occurred when parsing response: ", e.what(), ". response: ", response
                );
            }

            // TODO: Consider passing "reason" exception to stop
            m_connection->stop([](std::exception_ptr) {}, std::current_exception());
        }
    }

    bool hub_connection_impl::invoke_callback(completion_message* completion)
    {
        const char* error = nullptr;
        if (!completion->error.empty())
        {
            error = completion->error.data();
        }

        if (!m_callback_manager.invoke_complete_callback(completion->invocation_id, error, std::move(completion->result), true))
        {
            if (m_logger.is_enabled(trace_level::info))
            {
                m_logger.log(trace_level::info, std::string("no callback found for id: ").append(completion->invocation_id));
            }
        }

        return true;
    }

    void hub_connection_impl::handle_invoke_request(const invocation_message* invocation) const {
        std::function<value(const std::vector<value>&)> event_handler = nullptr;
        {
            std::lock_guard<std::mutex> lock(m_subscriptions_mutex);
            auto event = m_subscriptions.find(invocation->target);
            if (event != m_subscriptions.end())
            {
                event_handler = event->second;
            }
        }
        if (event_handler != nullptr) {
            const auto& args = invocation->arguments;
            signalr::value event_resp;
            try {
                event_resp = event_handler(args);
            }
            catch (const std::exception& e)
            {
                // Send completion message w/ invocation ID & error message.
                send_invoke_completion(invocation->invocation_id, value(), std::make_exception_ptr(e));
                return;
            }

            try {
                send_invoke_completion(invocation->invocation_id, event_resp, nullptr);
            }
            catch (const std::exception& e) {
                if (m_logger.is_enabled(trace_level::error)) {
                    std::stringstream ss;
                    ss << "Encountered error while sending invoke completion response, '" << event_resp
                       << "', w/ invocationID=" << invocation->invocation_id << ": " << e.what();
                    m_logger.log(trace_level::error, ss.str());
                }
            }
            catch (...) {
                if (m_logger.is_enabled(trace_level::error)) {
                    std::stringstream ss;
                    ss << "Encountered unknown error while sending invoke completion response, '"
                       << event_resp << "', w/ invocationID=" << invocation->invocation_id << ": ";
                    m_logger.log(trace_level::error, ss.str());
                }
            }
        } else {
            m_logger.log(trace_level::info, "handler not found");
            send_invoke_completion(invocation->invocation_id, value(),
                                   std::make_exception_ptr(signalr_exception("handler not found")));
        }
    }

    void hub_connection_impl::send_invoke_completion(
        const std::string& invocation_id, const signalr::value& response, const std::exception_ptr& err
    ) const {
        try {
            std::unique_ptr<completion_message> compl_msg;
            if (err != nullptr) {
                compl_msg = std::unique_ptr<completion_message>(
                    new completion_message(
                        invocation_id, formatException(err), response, completion_message::result_kind::error_result
                    )
                );
            } else {
                compl_msg = std::unique_ptr<completion_message>(
                    new completion_message(
                        invocation_id, "", response,
                        response.is_null() ? completion_message::result_kind::void_result
                                           : completion_message::result_kind::non_void_result
                    )
                );
            }
            const auto message = m_protocol->write_completion_message(compl_msg.get());

            const auto logger = m_logger;
            if (err != nullptr) {
                m_connection->send(
                    message, m_protocol->transfer_format(),
                    [logger, invocation_id, err](const std::exception_ptr& e) {
                        if (e != nullptr) {
                            logger.logv(
                                trace_level::error, "Failed to send invoke completion error response, '",
                                formatException(err), "', w/ invocationID=",
                                invocation_id, ": ", formatException(e)
                            );
                        } else {
                            logger.logv(
                                trace_level::info,
                                "Sent invoke completion error response w/ invocationID=", invocation_id
                            );
                        }
                    }
                );
            } else if (response.is_null()) {
                m_connection->send(
                    message, m_protocol->transfer_format(),
                    [logger, invocation_id](const std::exception_ptr& e) {
                        if (e != nullptr) {
                            logger.logv(
                                trace_level::error, "Failed to send invoke completion response w/ invocationID=",
                                invocation_id, ": ", formatException(e)
                            );
                        } else {
                            logger.logv(
                                trace_level::info, "Sent invoke completion void response w/ invocationID=", invocation_id
                            );
                        }
                    }
                );
            } else {
                m_connection->send(
                    message, m_protocol->transfer_format(),
                    [logger, invocation_id, response](const std::exception_ptr& e) {
                        if (e != nullptr) {
                            if (!logger.is_enabled(trace_level::error)) {
                                return;
                            }
                            std::stringstream ss;
                            ss << "Failed to send invoke completion response, '" << response << "', w/ invocationID="
                               << invocation_id << ": " << formatException(e);
                            logger.log(trace_level::error, ss.str());
                        } else {
                            logger.logv(
                                trace_level::info, "Sent invoke completion result response w/ invocationID=", invocation_id
                            );
                        }
                    }
                );
            }
        }
        catch (std::exception& e)
        {
            if (!m_logger.is_enabled(trace_level::error)) {
                return;
            }
            if (err != nullptr) {
                m_logger.logv(
                    trace_level::error, "Encountered error while sending invoke completion error response, '",
                    formatException(err), "' w/ invocationID=", invocation_id, ": ", e.what()
                );
            } else if (response.is_null()) {
                m_logger.logv(
                    trace_level::error,
                    "Encountered error while sending invoke completion void response w/ invocationID=",
                    invocation_id, ": ", e.what()
                );
            } else {
                std::stringstream ss;
                ss << "Encountered error while sending invoke completion response, '" << response
                   << "', w/ invocationID=" << invocation_id << ": " << e.what();
                m_logger.log(trace_level::error, ss.str());
            }
        }
        catch (...)
        {
            if (!m_logger.is_enabled(trace_level::error)) {
                return;
            }
            if (err != nullptr) {
                m_logger.logv(
                    trace_level::error, "Encountered unknown error while sending invoke completion error response, '",
                    formatException(err), "' w/ invocationID=", invocation_id
                );
            } else if (response.is_null()) {
                m_logger.logv(
                    trace_level::error,
                    "Encountered unknown error while sending invoke completion void response w/ invocationID=",
                    invocation_id
                );
            } else {
                std::stringstream ss;
                ss << "Encountered unknown error while sending invoke completion response, '" << response
                   << "', w/ invocationID=" << invocation_id;
                m_logger.log(trace_level::error, ss.str());
            }
        }
    }

    void hub_connection_impl::invoke(
        const std::string& method_name, const std::vector<signalr::value>& arguments,
        std::function<void(signalr::value&&, std::exception_ptr)> callback
    ) noexcept
    {
        const auto& callback_id = m_callback_manager.register_complete_callback(
            create_hub_invocation_callback(callback)
        );
        try {
            const invocation_message invocation(callback_id, method_name, arguments);
            const auto message = m_protocol->write_invocation_message(&invocation);

            // weak_ptr prevents a circular dependency leading to memory leak and other problems
            std::weak_ptr<hub_connection_impl> weak_hub_connection = shared_from_this();

            m_connection->send(
                message, m_protocol->transfer_format(),
                [callback, weak_hub_connection, callback_id](const std::exception_ptr& exception) {
                    if (exception) {
                        const auto hub_connection = weak_hub_connection.lock();
                        if (
                            hub_connection != nullptr &&
                            hub_connection->m_callback_manager.remove_complete_callback(callback_id)
                        ) {
                            callback(value(), exception);
                        }
                    }
                }
            );

            reset_send_ping();
        } catch (const std::exception& e) {
            m_callback_manager.remove_complete_callback(callback_id);
            if (m_logger.is_enabled(trace_level::warning))
            {
                m_logger.logv(trace_level::warning, "failed to invoke invocation: ", e.what());
            }
            callback(value(), std::current_exception());
        }
    }

    void hub_connection_impl::send(
        const std::string& method_name, const std::vector<signalr::value>& arguments,
        const std::function<void(std::exception_ptr)>& callback
    ) noexcept
    {
        try {
            const invocation_message invocation("", method_name, arguments);
            const auto message = m_protocol->write_invocation_message(&invocation);

            // weak_ptr prevents a circular dependency leading to memory leak and other problems
            std::weak_ptr<hub_connection_impl> weak_hub_connection = shared_from_this();

            m_connection->send(message, m_protocol->transfer_format(), callback);

            reset_send_ping();
        } catch (const std::exception& e) {
            if (m_logger.is_enabled(trace_level::warning))
            {
                m_logger.logv(trace_level::warning, "failed to send invocation: ", e.what());
            }
            callback(std::current_exception());
        }
    }

    std::unique_ptr<cancel_stream_interface> hub_connection_impl::stream(
        const std::string& method_name,
        const std::vector<signalr::value>& arguments, const std::function<void(signalr::value&&)>& handle_item_callback,
        const std::function<void(value&&, const std::exception_ptr&)>& completion_callback
    ) {
        const auto& callback_id = m_callback_manager.register_stream_callbacks(
            handle_item_callback, create_hub_invocation_callback(completion_callback)
        );

        try {
            const stream_invocation_message stream_invocation_msg(callback_id, method_name, arguments);
            const auto message = m_protocol->write_stream_invocation_message(&stream_invocation_msg);

            std::weak_ptr<hub_connection_impl> weak_hub_connection = shared_from_this();

            m_connection->send(
                message, m_protocol->transfer_format(),
                [completion_callback, weak_hub_connection, callback_id](const std::exception_ptr& exception) {
                    if (exception != nullptr) {

                    }
                }
            );
        }
        catch (const std::exception& e)
        {
            m_callback_manager.remove_complete_callback(callback_id);
            if (m_logger.is_enabled(trace_level::warning))
            {
                m_logger.logv(trace_level::warning, "failed to send stream invocation: ", e.what());
            }
            completion_callback(value(), std::current_exception());
        }
    }

    connection_state hub_connection_impl::get_connection_state() const noexcept
    {
        return m_connection->get_connection_state();
    }

    std::string hub_connection_impl::get_connection_id() const
    {
        return m_connection->get_connection_id();
    }

    void hub_connection_impl::set_client_config(const signalr_client_config& config)
    {
        m_signalr_client_config = config;
        m_connection->set_client_config(config);
    }

    void hub_connection_impl::set_disconnected(const std::function<void(std::exception_ptr)>& disconnected)
    {
        m_disconnected = disconnected;
    }

    void hub_connection_impl::reset_send_ping()
    {
        auto timeMs = (std::chrono::steady_clock::now() + m_signalr_client_config.get_keepalive_interval()).time_since_epoch();
        m_nextActivationSendPing.store(std::chrono::duration_cast<std::chrono::milliseconds>(timeMs).count());
    }

    void hub_connection_impl::reset_server_timeout()
    {
        auto timeMs = (std::chrono::steady_clock::now() + m_signalr_client_config.get_server_timeout()).time_since_epoch();
        m_nextActivationServerTimeout.store(std::chrono::duration_cast<std::chrono::milliseconds>(timeMs).count());
    }

    void hub_connection_impl::start_keepalive()
    {
        if (m_logger.is_enabled(trace_level::debug))
        {
            m_logger.log(trace_level::debug, "starting keep alive timer.");
        }

        auto send_ping = [](std::shared_ptr<hub_connection_impl> connection)
        {
            if (!connection)
            {
                return;
            }

            if (connection->get_connection_state() != connection_state::connected)
            {
                return;
            }

            try
            {
                std::weak_ptr<hub_connection_impl> weak_connection = connection;
                connection->m_connection->send(
                    connection->m_cached_ping,
                    connection->m_protocol->transfer_format(), [weak_connection](std::exception_ptr exception)
                    {
                        auto connection = weak_connection.lock();
                        if (connection)
                        {
                            if (exception)
                            {
                                if (connection->m_logger.is_enabled(trace_level::warning))
                                {
                                    connection->m_logger.log(trace_level::warning, "failed to send ping!");
                                }
                            }
                            else
                            {
                                connection->reset_send_ping();
                            }
                        }
                    });
            }
            catch (const std::exception& e)
            {
                if (connection->m_logger.is_enabled(trace_level::warning))
                {
                    connection->m_logger.log(trace_level::warning, std::string("failed to send ping: ").append(e.what()));
                }
            }
        };

        send_ping(shared_from_this());
        reset_server_timeout();

        std::weak_ptr<hub_connection_impl> weak_connection = shared_from_this();
        timer(m_signalr_client_config.get_scheduler(),
            [send_ping, weak_connection](std::chrono::milliseconds)
            {
                auto connection = weak_connection.lock();

                if (!connection)
                {
                    return true;
                }

                if (connection->get_connection_state() != connection_state::connected)
                {
                    return true;
                }

                auto timeNowmSeconds =
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();

                if (timeNowmSeconds > connection->m_nextActivationServerTimeout.load())
                {
                    if (connection->get_connection_state() == connection_state::connected)
                    {
                        auto error_msg = std::string("server timeout (")
                            .append(std::to_string(connection->m_signalr_client_config.get_server_timeout().count()))
                            .append(" ms) elapsed without receiving a message from the server.");
                        if (connection->m_logger.is_enabled(trace_level::warning))
                        {
                            connection->m_logger.log(trace_level::warning, error_msg);
                        }

                        connection->m_connection->stop([](std::exception_ptr)
                            {
                            }, std::make_exception_ptr(signalr_exception(error_msg)));
                    }
                }

                if (timeNowmSeconds > connection->m_nextActivationSendPing.load())
                {
                    if (connection->m_logger.is_enabled(trace_level::debug))
                    {
                        connection->m_logger.log(trace_level::debug, "sending ping to server.");
                    }
                    send_ping(connection);
                }

                return false;
            });
    }

    // unnamed namespace makes it invisible outside this translation unit
    namespace
    {
        std::function<void(const char* error, value&&)> create_hub_invocation_callback(
            const std::function<void(value&&, std::exception_ptr)>& set_result
        )
        {
            return [set_result](const char* error, signalr::value&& message)
            {
                if (error != nullptr)
                {
                    set_result(nullptr, std::make_exception_ptr(hub_exception(error)));
                }
                else
                {
                    set_result(std::move(message), nullptr);
                }
            };
        }
    }
}
