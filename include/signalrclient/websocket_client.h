// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "signalrclient/transfer_format.h"
#include <exception>
#include <functional>
#include <string>

namespace signalr {

/**
 * Interface representing the underlying full-duplex WebSocket client used by websocket_transport.
 *
 * Contract Requirements for Concrete Implementations:
 *  1. At-Most-Once Callback Execution:
 *     - Every operation (start, stop, send, receive) MUST invoke its provided callback at most once.
 *     - Duplicate invocations of the same callback for a single operation are strictly prohibited.
 *  2. No Throwing Post-Callback:
 *     - Once an operation's completion callback has been invoked (reporting success or failure),
 *       the method MUST NOT throw a synchronous exception afterward.
 *  3. Stop Quiescence:
 *     - Invoking stop() MUST cancel or drain any active in-flight receive() operations.
 *  4. Lock Safety & Non-Blocking Invocations:
 *     - Completion callbacks may be invoked synchronously or asynchronously.
 *     - Implementations MUST NOT hold internal client mutexes across completion callback invocations
 *       to prevent deadlocks when callbacks synchronously re-enter transport methods (e.g. calling stop()
 *       from inside a send() completion callback).
 *  5. Thread Safety:
 *     - The implementation must support concurrent calls to send(), receive(), and stop() across
 *       different threads.
 */
class websocket_client {
public:
    virtual ~websocket_client() = default;

    /**
     * Establishes a WebSocket connection to the designated URL.
     *
     * @param url The target WebSocket URL.
     * @param callback Completion callback invoked with nullptr on success, or an error exception_ptr.
     */
    virtual void start(
        const std::string& url,
        const std::function<void(const std::exception_ptr&)>& callback
    ) = 0;

    /**
     * Closes the active WebSocket connection.
     *
     * @param callback Completion callback invoked when the socket is closed.
     */
    virtual void stop(const std::function<void(const std::exception_ptr&)>& callback) = 0;

    /**
     * Transmits a frame across the WebSocket connection.
     *
     * @param payload The data to transmit.
     * @param transfer_format Specifies text or binary framing.
     * @param callback Completion callback invoked upon delivery confirmation or write error.
     */
    virtual void send(
        const std::string& payload,
        transfer_format transfer_format,
        const std::function<void(std::exception_ptr)>& callback
    ) = 0;

    /**
     * Registers a single read operation to receive the next inbound frame.
     *
     * @param callback Callback invoked exactly once when a message arrives or an error/EOF occurs.
     */
    virtual void receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) = 0;
};

} // namespace signalr
