// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "logger.h"
#include "signalrclient/transfer_format.h"
#include "signalrclient/transport_type.h"
#include <functional>
#include <string>

namespace signalr {

class transport {
public:
    // Do NOT remove this destructor. Letting the compiler generate and inline the default dtor may lead to
    // undefined behavior since we are using an incomplete type. More details here:  http://herbsutter.com/gotw/_100/
    virtual ~transport() = default;

    virtual transport_type get_transport_type() const noexcept = 0;

    virtual void start(
        const std::string& url,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept = 0;

    virtual void stop(const std::function<void(const std::exception_ptr&)>& callback) noexcept = 0;

    virtual void on_close(const std::function<void(const std::exception_ptr&)>& callback) = 0;

    virtual void send(
        const std::string& payload,
        transfer_format transfer_format,
        const std::function<void(const std::exception_ptr&)>& callback
    ) noexcept = 0;

    virtual void on_receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) = 0;

protected:
    explicit transport(logger logger) : m_logger(std::move(logger)) {}

    logger m_logger;
};

} // namespace signalr