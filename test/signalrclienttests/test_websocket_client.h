// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "stdafx.h"
#include <functional>
#include "signalrclient/websocket_client.h"
#include "test_utils.h"
#include "cancellation_token_source.h"
#include <queue>
#include "signalrclient/signalr_client_config.h"

using namespace signalr;

class test_websocket_client : public websocket_client
{
public:
    test_websocket_client();
    ~test_websocket_client() override;

    void set_config(const signalr_client_config&);

    void start(const std::string& url, const std::function<void(const std::exception_ptr&)>& callback) override;

    void stop(const std::function<void(const std::exception_ptr&)>& callback) override;

    void send(const std::string& payload, signalr::transfer_format transfer_format, const std::function<void(std::exception_ptr)>&
              callback) override;

    void receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) override;

    void set_connect_function(const std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>& connect_function);

    void set_send_function(const std::function<void(const std::string& msg, std::function<void(std::exception_ptr)>)>& send_function);

    void set_close_function(const std::function<void(std::function<void(std::exception_ptr)>)>& close_function);

    void receive_message(const std::string& message);
    void receive_message(std::exception_ptr ex);

    cancellation_token_source receive_loop_started;
    cancellation_token_source handshake_sent;
    int receive_count;
    bool ignore_pings;

private:
    std::shared_ptr<std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>> m_connect_function;

    std::shared_ptr < std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>> m_send_function;

    std::shared_ptr < std::function<void(std::function<void(std::exception_ptr)>)>> m_close_function;

    std::exception_ptr m_receive_exception;
    std::string m_receive_message;

    manual_reset_event<bool> m_receive_message_event;
    std::mutex m_receive_lock;
    bool m_stopped;
    manual_reset_event<void> m_receive_waiting;
    cancellation_token_source m_receive_loop_not_running;
    std::shared_ptr<scheduler> m_scheduler;
};

std::shared_ptr<test_websocket_client> create_test_websocket_client(
    const std::function<void(const std::string & msg, std::function<void(std::exception_ptr)>)>& send_function = [](const std::string&, std::function<void(std::exception_ptr)> callback) { callback(nullptr); },
    const std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>& connect_function = [](const std::string&, std::function<void(std::exception_ptr)> callback) { callback(nullptr); },
    const std::function<void(std::function<void(std::exception_ptr)>)>& close_function = [](std::function<void(std::exception_ptr)> callback) { callback(nullptr); }, bool ignore_pings = true);