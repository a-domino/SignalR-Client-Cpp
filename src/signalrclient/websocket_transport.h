// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "logger.h"
#include "signalrclient/signalr_client_config.h"
#include "signalrclient/websocket_client.h"
#include "transport.h"
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace signalr {

class connection_session;

/**
 * Concrete SignalR transport operating over a full-duplex WebSocket connection.
 *
 * Architectural & Lifecycle Guarantees:
 *  - Per-Connection Session Isolation: Each call to start() allocates an independent connection_session
 *    with a unique monotonic session ID. Reconnecting, retrying after a failed connection, or tearing down
 *    an old session never leaks state or interferes with subsequent sessions.
 *  - Atomic Close Handoff: The active session pointer is retained until the close notification is fully
 *    dispatched under transport locks, guaranteeing that on_close is never lost due to concurrent start calls.
 *  - Reentrant Reconnection: on_close handlers may synchronously call start() without encountering deadlocks.
 *  - Stack-Safe Receive Trampoline: High-frequency inline message deliveries are dispatched via an iterative
 *    trampoline loop, bounding call-stack depth regardless of stream volume.
 *  - Non-Blocking Teardown: Destructors cleanly signal session shutdown without risking infinite thread hangs.
 */
class websocket_transport : public transport, public std::enable_shared_from_this<websocket_transport> {
public:
    /**
     * Instantiates a shared websocket_transport.
     *
     * @param websocket_client_factory Factory functor to create underlying websocket_client instances.
     * @param signalr_client_config Configuration settings including timeouts, headers, and scheduler.
     * @param logger Diagnostic logger facility.
     * @return Shared pointer to the new transport.
     */
    static std::shared_ptr<transport> create(
        const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_client_factory,
        const signalr_client_config& signalr_client_config,
        const logger& logger
    );

    /**
     * Destructor. Signals shutdown on the active session without blocking indefinitely.
     */
    ~websocket_transport() override;

    websocket_transport(const websocket_transport&) = delete;

    websocket_transport& operator=(const websocket_transport&) = delete;

    /**
     * Returns transport_type::websockets.
     */
    transport_type get_transport_type() const noexcept override;

    /**
     * Connects to the server over WebSockets.
     *
     * Reserves an isolated session before invoking the client factory. If stop() is called while the factory
     * or connection is in progress, the start attempt cancels safely without leaking open connections.
     *
     * @param url The endpoint URL (ws:// or wss://).
     * @param callback Invoked upon completion (nullptr on success, exception_ptr on failure).
     */
    void start(
        const std::string& url,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept override;

    /**
     * Stops the active connection session.
     *
     * Closes the client socket, ceases message dispatch, and notifies all registered stop/close callbacks.
     *
     * @param callback Invoked when the transport has completely stopped.
     */
    void stop(const std::function<void(const std::exception_ptr&)>& callback) noexcept override;

    /**
     * Sets the callback invoked when the connection terminates.
     *
     * @param callback Invoked with the terminating error or nullptr if closed cleanly.
     */
    void on_close(const std::function<void(const std::exception_ptr&)>& callback) override;

    /**
     * Sends data across the WebSocket connection.
     *
     * Transmits data outside of mutex locks to eliminate deadlocks with synchronous send callbacks.
     *
     * @param payload The serialized message payload.
     * @param transfer_format Specifies text or binary framing.
     * @param callback Invoked upon send confirmation or failure.
     */
    void send(
        const std::string& payload,
        transfer_format transfer_format,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept override;

    /**
     * Registers the callback invoked when a complete message frame arrives.
     *
     * @param callback Invoked with the inbound message payload.
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
