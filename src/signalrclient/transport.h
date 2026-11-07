// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "logger.h"
#include "signalrclient/transport_type.h"
#include "signalrclient/transfer_format.h"
#include <exception>
#include <functional>
#include <string>

namespace signalr {

/**
 * Abstract base class representing a SignalR client network transport.
 *
 * Contract Requirements for Callers (such as connection_impl):
 *  1. Single Active Connection Lifecycle:
 *     - A transport instance hosts at most one active connection session at a time.
 *     - Calling start() while the transport is already connected, connecting, or disconnecting
 *       fails immediately with a signalr_exception passed to the completion callback.
 *  2. Terminal State and Close Notification Guarantees:
 *     - The on_close callback is invoked if and only if the transport previously reached the
 *       'connected' state and subsequently terminated (either via a network error, remote disconnect,
 *       or explicit stop()).
 *     - Connection attempts that fail during start() will NOT trigger on_close; they only report
 *       their failure through the start() completion callback.
 *  3. Synchronous Reconnection Contract:
 *     - Callers are explicitly permitted to initiate a new connection by calling start() synchronously
 *       from inside the on_close callback. The transport guarantees that the previous session has been
 *       cleared from active ownership before on_close is invoked.
 *  4. Lock-Free Callback Dispatch:
 *     - All user-supplied callbacks (start, stop, send, on_receive, on_close) are invoked strictly
 *       outside of internal transport locks, ensuring reentrancy without self-deadlock.
 */
class transport {
public:
    explicit transport(const logger& logger) : m_logger(logger) {}

    virtual ~transport() = default;

    transport(const transport&) = delete;

    transport& operator=(const transport&) = delete;

    /**
     * Retrieves the transport type enum value identifying the underlying implementation.
     */
    virtual transport_type get_transport_type() const noexcept = 0;

    /**
     * Initiates an asynchronous connection to the target URL.
     *
     * @param url The endpoint URL.
     * @param callback Completion callback invoked exactly once (with nullptr on success or an error).
     */
    virtual void start(
        const std::string& url,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept = 0;

    /**
     * Asynchronously terminates the active connection.
     *
     * In-flight sends and receive loops are stopped, and all registered stop callbacks are executed.
     * Multiple concurrent calls to stop() are coalesced so all callers receive notification upon termination.
     *
     * @param callback Completion callback invoked when transport shutdown has finished.
     */
    virtual void stop(const std::function<void(const std::exception_ptr&)>& callback) noexcept = 0;

    /**
     * Registers a callback invoked whenever an established connection terminates.
     *
     * @param callback Function invoked with the terminating exception_ptr (or nullptr if stopped cleanly).
     */
    virtual void on_close(const std::function<void(const std::exception_ptr&)>& callback) = 0;

    /**
     * Transmits a message payload across the transport connection.
     *
     * @param payload The encoded data to send.
     * @param transfer_format Specifies whether the frame is text or binary.
     * @param callback Completion callback invoked upon delivery confirmation or error.
     */
    virtual void send(
        const std::string& payload,
        transfer_format transfer_format,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept = 0;

    /**
     * Registers a callback invoked whenever an inbound message payload arrives from the transport.
     *
     * @param callback Callback receiving the message payload string and optional exception pointer.
     */
    virtual void on_receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) = 0;

protected:
    logger m_logger;
};

} // namespace signalr
