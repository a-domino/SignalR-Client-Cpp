// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "cancellation_token_source.h"
#include "signalrclient/signalr_client_config.h"
#include "signalrclient/websocket_client.h"
#include "test_utils.h"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

namespace signalr {

class test_websocket_client : public websocket_client {
public:
    using connect_fn_t = std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>;
    using send_fn_t = std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>;
    using close_fn_t = std::function<void(std::function<void(std::exception_ptr)>)>;

    test_websocket_client();
    ~test_websocket_client() override;

    void set_config(const signalr_client_config& config);

    void start(
        const std::string& url,
        const std::function<void(const std::exception_ptr&)>& callback
    ) override;

    void stop(const std::function<void(const std::exception_ptr&)>& callback) override;

    void send(
        const std::string& payload,
        transfer_format transfer_format,
        const std::function<void(std::exception_ptr)>& callback
    ) override;

    void receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) override;

    void set_connect_function(const connect_fn_t& connect_function);
    void set_send_function(const send_fn_t& send_function);
    void set_close_function(const close_fn_t& close_function);

    void receive_message(const std::string& message) const;
    void receive_message(std::exception_ptr ex) const;

    cancellation_token_source receive_loop_started;
    cancellation_token_source handshake_sent;
    std::atomic<int> receive_count{0};
    std::atomic<bool> ignore_pings{true};

private:
    struct message_or_error {
        std::string payload;
        std::exception_ptr error;

        message_or_error()
            : payload(), error(nullptr) {}

        message_or_error(std::string p, std::exception_ptr e)
            : payload(std::move(p)), error(std::move(e)) {}
    };

    struct shared_state {
        std::mutex lock;
        std::condition_variable cv;
        std::condition_variable receive_registered_cv;
        std::condition_variable message_processed_cv;
        bool stopped{true};
        bool waiting_for_receive{false};
        uint64_t messages_queued{0};
        uint64_t messages_completed{0};
        std::thread::id active_worker_id;
        std::queue<message_or_error> inbound_queue;
        cancellation_token_source receive_loop_not_running;
    };

    std::shared_ptr<shared_state> m_state;
    std::shared_ptr<scheduler> m_scheduler;

    mutable std::mutex m_callback_lock;
    std::shared_ptr<connect_fn_t> m_connect_function;
    std::shared_ptr<send_fn_t> m_send_function;
    std::shared_ptr<close_fn_t> m_close_function;
};

std::shared_ptr<test_websocket_client> create_test_websocket_client(
    const std::function<void(const std::string& msg, std::function<void(std::exception_ptr)>)>& send_function =
        [](const std::string&, const std::function<void(std::exception_ptr)>& callback) {
            callback(nullptr);
        },
    const std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>& connect_function =
        [](const std::string&, const std::function<void(std::exception_ptr)>& callback) {
            callback(nullptr);
        },
    const std::function<void(std::function<void(std::exception_ptr)>)>& close_function =
        [](const std::function<void(std::exception_ptr)>& callback) {
            callback(nullptr);
        },
    bool ignore_pings = true
);

} // namespace signalr
