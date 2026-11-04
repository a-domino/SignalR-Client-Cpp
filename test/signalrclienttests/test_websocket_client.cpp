// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "test_websocket_client.h"
#include "test_utils.h"
#include <thread>
#include <utility>

namespace signalr {

std::shared_ptr<test_websocket_client> create_test_websocket_client(
    const std::function<void(const std::string& msg, std::function<void(std::exception_ptr)>)>& send_function,
    const std::function<void(const std::string&, std::function<void(std::exception_ptr)>)>& connect_function,
    const std::function<void(std::function<void(std::exception_ptr)>)>& close_function,
    bool ignore_pings
) {
    auto websocket_client = std::make_shared<test_websocket_client>();
    websocket_client->set_send_function(send_function);
    websocket_client->set_connect_function(connect_function);
    websocket_client->set_close_function(close_function);
    websocket_client->ignore_pings.store(ignore_pings);
    return websocket_client;
}

test_websocket_client::test_websocket_client()
    : m_state(std::make_shared<shared_state>()),
      m_connect_function(
          std::make_shared<connect_fn_t>(
              [](const std::string&, const std::function<void(std::exception_ptr)>& cb) {
                  cb(nullptr);
              }
          )
      ),
      m_send_function(
          std::make_shared<send_fn_t>(
              [](const std::string&, const std::function<void(std::exception_ptr)>& cb) {
                  cb(nullptr);
              }
          )
      ),
      m_close_function(
          std::make_shared<close_fn_t>(
              [](const std::function<void(std::exception_ptr)>& cb) {
                  cb(nullptr);
              }
          )
      ) {
    m_state->receive_loop_not_running.cancel();
}

test_websocket_client::~test_websocket_client() {
    {
        std::lock_guard<std::mutex> lock(m_state->lock);
        m_state->stopped = true;
        m_state->cv.notify_all();
        m_state->receive_registered_cv.notify_all();
        m_state->message_processed_cv.notify_all();
    }
    m_state->receive_loop_not_running.wait(1000);
}

void test_websocket_client::set_config(const signalr_client_config& config) {
    m_scheduler = config.get_scheduler();
}

void test_websocket_client::start(
    const std::string& url,
    const std::function<void(const std::exception_ptr&)>& callback
) {
    {
        std::lock_guard<std::mutex> lock(m_state->lock);
        m_state->stopped = false;
        m_state->waiting_for_receive = false;
        m_state->messages_queued = 0;
        m_state->messages_completed = 0;
        m_state->active_worker_id = std::thread::id();
        while (!m_state->inbound_queue.empty()) {
            m_state->inbound_queue.pop();
        }
        m_state->receive_loop_not_running.cancel();
    }

    handshake_sent.reset();
    receive_loop_started.reset();

    std::shared_ptr<connect_fn_t> connect_fn;
    {
        std::lock_guard<std::mutex> lock(m_callback_lock);
        connect_fn = m_connect_function;
    }

    m_scheduler->schedule(
        [url, callback, connect_fn]() {
            try {
                (*connect_fn)(url, callback);
            } catch (...) {
                callback(std::current_exception());
            }
        }
    );
}

void test_websocket_client::stop(const std::function<void(const std::exception_ptr&)>& callback) {
    bool is_worker_thread;
    {
        std::lock_guard<std::mutex> lock(m_state->lock);
        is_worker_thread = (std::this_thread::get_id() == m_state->active_worker_id);
        if (!m_state->stopped) {
            m_state->stopped = true;
            m_state->cv.notify_all();
            m_state->receive_registered_cv.notify_all();
            m_state->message_processed_cv.notify_all();
        }
    }

    if (!is_worker_thread) {
        m_state->receive_loop_not_running.wait(1000);
    }

    handshake_sent.reset();
    receive_loop_started.reset();

    std::shared_ptr<close_fn_t> close_fn;
    {
        std::lock_guard<std::mutex> lock(m_callback_lock);
        close_fn = m_close_function;
    }

    m_scheduler->schedule(
        [callback, close_fn]() {
            try {
                (*close_fn)(callback);
            } catch (...) {
                callback(std::current_exception());
            }
        }
    );
}

void test_websocket_client::send(
    const std::string& payload,
    signalr::transfer_format,
    const std::function<void(std::exception_ptr)>& callback
) {
    handshake_sent.cancel();

    std::shared_ptr<send_fn_t> send_fn;
    {
        std::lock_guard<std::mutex> lock(m_callback_lock);
        send_fn = m_send_function;
    }

    auto l_ignore_pings = ignore_pings.load();
    m_scheduler->schedule(
        [payload, callback, send_fn, l_ignore_pings]() {
            try {
                if (l_ignore_pings && payload.find("\"type\":6") != std::string::npos) {
                    callback(nullptr);
                    return;
                }
                (*send_fn)(payload, callback);
            } catch (...) {
                callback(std::current_exception());
            }
        }
    );
}

void test_websocket_client::receive(
    const std::function<void(std::string&&, const std::exception_ptr&)>& callback
) {
    ++receive_count;
    receive_loop_started.cancel();

    {
        std::lock_guard<std::mutex> lock(m_state->lock);
        m_state->receive_loop_not_running.reset();
        m_state->waiting_for_receive = true;
        m_state->receive_registered_cv.notify_all();
    }

    auto state = m_state;
    std::thread(
        [callback, state]() {
            std::unique_lock<std::mutex> lock(state->lock);
            state->cv.wait(
                lock,
                [state]() {
                    return state->stopped || !state->inbound_queue.empty();
                }
            );

            state->waiting_for_receive = false;

            if (state->stopped && state->inbound_queue.empty()) {
                state->active_worker_id = std::this_thread::get_id();
                lock.unlock();
                try {
                    callback("", nullptr);
                } catch (...) {}

                {
                    std::lock_guard<std::mutex> lock_completed(state->lock);
                    state->active_worker_id = std::thread::id();
                    state->message_processed_cv.notify_all();
                }

                state->receive_loop_not_running.cancel();
                return;
            }

            auto item = std::move(state->inbound_queue.front());
            state->inbound_queue.pop();
            state->active_worker_id = std::this_thread::get_id();
            lock.unlock();

            try {
                callback(std::move(item.payload), item.error);
            } catch (...) {}

            {
                std::lock_guard<std::mutex> lock_completed(state->lock);
                state->active_worker_id = std::thread::id();
                ++state->messages_completed;
                state->message_processed_cv.notify_all();
            }

            state->receive_loop_not_running.cancel();
        }
    ).detach();
}

void test_websocket_client::set_connect_function(const connect_fn_t& connect_function) {
    std::lock_guard<std::mutex> lock(m_callback_lock);
    m_connect_function = std::make_shared<connect_fn_t>(connect_function);
}

void test_websocket_client::set_send_function(const send_fn_t& send_function) {
    std::lock_guard<std::mutex> lock(m_callback_lock);
    m_send_function = std::make_shared<send_fn_t>(send_function);
}

void test_websocket_client::set_close_function(const close_fn_t& close_function) {
    std::lock_guard<std::mutex> lock(m_callback_lock);
    m_close_function = std::make_shared<close_fn_t>(close_function);
}

void test_websocket_client::receive_message(const std::string& message) const {
    {
        std::unique_lock<std::mutex> lock(m_state->lock);
        m_state->receive_registered_cv.wait_for(
            lock,
            std::chrono::seconds(5),
            [this]() {
                return m_state->waiting_for_receive || m_state->stopped;
            }
        );

        if (m_state->stopped) {
            return;
        }

        uint64_t id = ++m_state->messages_queued;
        m_state->inbound_queue.emplace(message, nullptr);
        m_state->cv.notify_one();

        if (std::this_thread::get_id() != m_state->active_worker_id) {
            m_state->message_processed_cv.wait_for(
                lock,
                std::chrono::seconds(5),
                [this, id]() {
                    return m_state->messages_completed >= id || m_state->stopped;
                }
            );
        }
    }
}

void test_websocket_client::receive_message(std::exception_ptr ex) const {
    {
        std::unique_lock<std::mutex> lock(m_state->lock);
        m_state->receive_registered_cv.wait_for(
            lock,
            std::chrono::seconds(5),
            [this]() {
                return m_state->waiting_for_receive || m_state->stopped;
            }
        );

        if (m_state->stopped) {
            return;
        }

        uint64_t id = ++m_state->messages_queued;
        m_state->inbound_queue.emplace("", std::move(ex));
        m_state->cv.notify_one();

        if (std::this_thread::get_id() != m_state->active_worker_id) {
            m_state->message_processed_cv.wait_for(
                lock,
                std::chrono::seconds(5),
                [this, id]() {
                    return m_state->messages_completed >= id || m_state->stopped;
                }
            );
        }
    }
}

} // namespace signalr
