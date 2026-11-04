// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "logger.h"
#include "signalrclient/signalr_client_config.h"
#include "signalrclient/websocket_client.h"
#include "transport.h"
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace signalr {

class connection_session;

/**
 * An implementation of the transport interface that operates over full-duplex WebSocket connections.
 *
 * Each active connection attempt is represented by an independent, isolated connection_session.
 * This ensures that reconnecting, retrying after a failed connection, or cleaning up abandoned
 * connections cannot leak state, deadlock, or interfere with subsequent connection attempts.
 */
class websocket_transport : public transport, public std::enable_shared_from_this<websocket_transport> {
public:
    /**
     * Factory function to create a new shared instance of websocket_transport.
     *
     * @param websocket_client_factory Factory functor to instantiate the underlying WebSocket client.
     * @param signalr_client_config Client configuration settings (timeouts, headers, scheduler).
     * @param logger Logging facility used for transport diagnostic output.
     * @return Shared pointer to the newly created transport instance.
     */
    static std::shared_ptr<transport> create(
        const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_client_factory,
        const signalr_client_config& signalr_client_config,
        const logger& logger
    );

    /**
     * Destructor. Signals shutdown on any active session without deadlocking or blocking indefinitely.
     */
    ~websocket_transport() override;

    websocket_transport(const websocket_transport&) = delete;
    websocket_transport& operator=(const websocket_transport&) = delete;

    /**
     * Returns the transport type enum for this transport instance (transport_type::websockets).
     */
    transport_type get_transport_type() const noexcept override;

    /**
     * Initiates an asynchronous connection to the specified WebSocket URL.
     *
     * Creates an isolated connection session and transitions from disconnected to connecting.
     * If the transport is currently connected or in the process of connecting/disconnecting,
     * the callback is invoked with a "transport already connected" error.
     *
     * @param url The WebSocket URL (must begin with "ws://" or "wss://").
     * @param callback Function invoked exactly once upon completion (with nullptr on success or an error).
     */
    void start(
        const std::string& url,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept override;

    /**
     * Asynchronously terminates the active connection session.
     *
     * Stops the underlying WebSocket client, stops message dispatch, and ensures all registered
     * stop and close callbacks are executed. Multiple concurrent stop calls are coalesced so that
     * all callers receive completion notification when shutdown is finalized.
     *
     * @param callback Function invoked when the transport has completely stopped.
     */
    void stop(const std::function<void(const std::exception_ptr&)>& callback) noexcept override;

    /**
     * Registers a callback invoked whenever the transport connection terminates.
     *
     * Thread-safe; replaces any previously registered close callback.
     *
     * @param callback Function invoked with nullptr if stopped gracefully, or an exception_ptr on error.
     */
    void on_close(const std::function<void(const std::exception_ptr&)>& callback) override;

    /**
     * Asynchronously transmits a message payload across the active WebSocket connection.
     *
     * Checks connection state and transmits outside of internal mutex locks to prevent deadlocks
     * with synchronous callbacks.
     *
     * @param payload String containing the encoded message data.
     * @param transfer_format Indicates whether the frame is text or binary.
     * @param callback Function invoked upon transmission completion (or failure).
     */
    void send(
        const std::string& payload,
        transfer_format transfer_format,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept override;

    /**
     * Registers a callback invoked when an inbound message arrives across the WebSocket connection.
     *
     * Thread-safe; replaces any previously registered receive callback.
     *
     * @param callback Function invoked with the received message data payload.
     */
    void on_receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) override;

private:
    friend class connection_session;

    websocket_transport(
        const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_client_factory,
        signalr_client_config signalr_client_config,
        const logger& logger
    );

    std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)> m_websocket_client_factory;
    signalr_client_config m_signalr_client_config;

    mutable std::mutex m_mutex;
    std::shared_ptr<connection_session> m_active_session;
    uint64_t m_next_session_id{0};

    std::function<void(std::string&&, const std::exception_ptr&)> m_process_response_callback;
    std::function<void(const std::exception_ptr&)> m_close_callback;

    void notify_receive(uint64_t session_id, std::string&& message) const;
    void notify_close(uint64_t session_id, const std::exception_ptr& exception);
    void clear_active_session(uint64_t session_id);
};

} // namespace signalr
