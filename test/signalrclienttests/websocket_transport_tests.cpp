// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "test_utils.h"
#include "trace_log_writer.h"
#include "test_websocket_client.h"
#include "websocket_transport.h"
#include "memory_log_writer.h"
#include <future>
#include <atomic>
#include <algorithm>
#include <utility>

using namespace signalr;

TEST(websocket_transport_connect, connect_connects_and_starts_receive_loop)
{
    auto connect_called = false;
    auto client = std::make_shared<test_websocket_client>();

    client->set_connect_function([&connect_called](const std::string&, const std::function<void(std::exception_ptr)>& callback)
    {
        connect_called = true;
        callback(nullptr);
    });

    std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(writer, trace_level::info));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org/connect?param=42", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });

    mre.get();

    ASSERT_TRUE(connect_called);
    ASSERT_FALSE(client->receive_loop_started.wait(5000));

    auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();
    ASSERT_FALSE(log_entries.empty());

    auto entry = remove_date_from_log_entry(log_entries[0]);
    ASSERT_EQ("[info     ] [websocket transport] connecting to: ws://fakeuri.org/connect\n", entry);

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
}

TEST(websocket_transport_connect, connect_propagates_exceptions)
{
    auto client = std::make_shared<test_websocket_client>();
    client->set_connect_function([](const std::string&, const std::function<void(std::exception_ptr)>& callback)
    {
        callback(std::make_exception_ptr(std::runtime_error("connecting failed")));
    });

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    try
    {
        auto mre = manual_reset_event<void>();
        ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
        {
            mre.set(exception);
        });
        mre.get();
        ASSERT_TRUE(false);
    }
    catch (const std::exception &e)
    {
        ASSERT_STREQ("connecting failed", e.what());
    }
}

TEST(websocket_transport_connect, connect_logs_exceptions)
{
    auto client = std::make_shared<test_websocket_client>();
    client->set_connect_function([](const std::string&, const std::function<void(std::exception_ptr)>& callback)
    {
        callback(std::make_exception_ptr(std::runtime_error("connecting failed")));
    });

    std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());
    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(writer, trace_level::error));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    try
    {
        mre.get();
    }
    catch (...) {}

    auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();

    ASSERT_FALSE(log_entries.empty());

    auto entry = remove_date_from_log_entry(log_entries[0]);

    ASSERT_EQ(
        "[error    ] [websocket transport] exception when connecting to the server: connecting failed\n",
        entry);
}

TEST(websocket_transport_connect, cannot_call_connect_on_already_connected_transport)
{
    auto client = std::make_shared<test_websocket_client>();
    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    try
    {
        ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
        {
            mre.set(exception);
        });
        mre.get();
        ASSERT_TRUE(false);
    }
    catch (const std::exception &e)
    {
        ASSERT_STREQ("transport already connected", e.what());
    }

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
}

TEST(websocket_transport_connect, can_connect_after_disconnecting)
{
    auto client = std::make_shared<test_websocket_client>();
    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
}

TEST(websocket_transport_send, send_creates_and_sends_websocket_messages)
{
    bool send_called = false;

    auto client = std::make_shared<test_websocket_client>();

    client->set_send_function([&send_called](const std::string&, const std::function<void(std::exception_ptr)>& callback)
    {
        send_called = true;
        callback(nullptr);
    });

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://url", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->send("ABC", transfer_format::text, [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ASSERT_TRUE(send_called);

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
}

TEST(websocket_transport_disconnect, disconnect_closes_websocket)
{
    bool close_called = false;

    auto client = std::make_shared<test_websocket_client>();

    client->set_close_function([&close_called](const std::function<void(std::exception_ptr)>& callback)
    {
        close_called = true;
        callback(nullptr);
    });

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://url", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ASSERT_TRUE(close_called);
}

TEST(websocket_transport_stop, propogates_exception_from_close)
{
    auto client = std::make_shared<test_websocket_client>();

    bool close_called = false;
    client->set_close_function([&close_called](const std::function<void(std::exception_ptr)>& callback)
    {
        close_called = true;
        callback(std::make_exception_ptr(std::exception()));
    });

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://url", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    try
    {
        mre.get();
        ASSERT_TRUE(false);
    }
    catch (...) { }

    ASSERT_TRUE(close_called);
}

TEST(websocket_transport_disconnect, disconnect_logs_exceptions)
{
    auto client = std::make_shared<test_websocket_client>();
    client->set_close_function([](const std::function<void(std::exception_ptr)>& callback)
    {
        callback(std::make_exception_ptr(std::runtime_error("connection closing failed")));
    });

    std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(writer, trace_level::error));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://url", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });

    try
    {
        mre.get();
        ASSERT_TRUE(false);
    }
    catch (...) {}

    auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();

    ASSERT_FALSE(log_entries.empty());

    // disconnect cancels the receive loop by setting the cancellation token source to cancelled which results in writing
    // to the log. Exceptions from close are also logged but this happens on a different thread. As a result the order
    // of messages in the log is not deterministic and therefore we just use the "contains" idiom to find the message.
    ASSERT_NE(std::find_if(log_entries.begin(), log_entries.end(), [](const std::string& entry)
        {
            return remove_date_from_log_entry(entry) ==
                "[error    ] websocket transport stopped with error: connection closing failed\n";
        }),
        log_entries.end());
}

TEST(websocket_transport_disconnect, receive_not_called_after_disconnect)
{
    auto client = std::make_shared<test_websocket_client>();

    client->set_close_function([](const std::function<void(std::exception_ptr)>& callback)
    {
        callback(nullptr);
    });

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr&)
    {
        mre.set();
    });
    mre.get();

    ASSERT_FALSE(client->receive_loop_started.wait(5000));

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
    ASSERT_FALSE(client->receive_loop_started.wait(5000));

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ASSERT_EQ(2, client->receive_count);
}

TEST(websocket_transport_disconnect, disconnect_is_no_op_if_transport_not_started)
{
    auto client = std::make_shared<test_websocket_client>();

    auto close_called = false;

    client->set_close_function([&close_called](const std::function<void(std::exception_ptr)>& callback)
    {
        close_called = true;
        callback(nullptr);
    });

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));

    auto mre = manual_reset_event<void>();
    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ASSERT_FALSE(close_called);
}

template<typename T>
static void receive_loop_logs_exception_runner(const T& e, const std::string& expected_message, trace_level trace_level);

TEST(websocket_transport_receive_loop, receive_loop_logs_std_exception)
{
    receive_loop_logs_exception_runner(
        std::runtime_error("exception"),
        "[error    ] [websocket transport] error receiving response from websocket: exception\n",
        trace_level::error);
}

template<typename T>
void receive_loop_logs_exception_runner(const T& e, const std::string& expected_message, trace_level trace_level)
{
    auto client = std::make_shared<test_websocket_client>();
    std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(writer, trace_level));

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://url", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    ASSERT_FALSE(client->receive_loop_started.wait(5000));

    auto close_mre = manual_reset_event<void>();
    ws_transport->on_close([&close_mre](const std::exception_ptr& exception)
    {
        close_mre.set(exception);
    });

    client->receive_message(std::make_exception_ptr(e));

    try
    {
        close_mre.get();
        ASSERT_TRUE(false);
    }
    catch (const std::exception & ex)
    {
        ASSERT_STREQ(e.what(), ex.what());
    }

    auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();

    ASSERT_NE(std::find_if(log_entries.begin(), log_entries.end(),
        [&expected_message](const std::string &entry) { return remove_date_from_log_entry(entry) == expected_message; }),
        log_entries.end()) << dump_vector(log_entries);
}

TEST(websocket_transport_receive_loop, process_response_callback_called_when_message_received)
{
    auto client = std::make_shared<test_websocket_client>();

    auto process_response_event = std::make_shared<cancellation_token_source>();
    auto msg = std::make_shared<std::string>();

    auto process_response = [msg, process_response_event](const std::string& message, const std::exception_ptr& exception)
    {
        ASSERT_FALSE(exception);
        *msg = message;
        process_response_event->cancel();
    };

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));
    ws_transport->on_receive(process_response);

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    client->receive_message("msg");

    process_response_event->wait(1000);

    ASSERT_EQ(std::string("msg"), *msg);

    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
}

TEST(websocket_transport_receive_loop, error_callback_called_when_exception_thrown)
{
    auto client = std::make_shared<test_websocket_client>();

    auto close_invoked = std::make_shared<bool>(false);
    client->set_close_function([close_invoked](const std::function<void(std::exception_ptr)>& callback)
    {
        *close_invoked = true;
        callback(nullptr);
    });

    auto error_event = std::make_shared<cancellation_token_source>();
    auto exception_msg = std::make_shared<std::string>();

    auto error_callback = [exception_msg, error_event](std::exception_ptr exception)
    {
        try
        {
            std::rethrow_exception(std::move(exception));
        }
        catch (const std::exception& e)
        {
            *exception_msg = e.what();
        }
        error_event->cancel();
    };

    auto ws_transport = websocket_transport::create([&](const signalr_client_config& config)
        {
            client->set_config(config);
            return client;
        }, signalr_client_config{}, logger(std::make_shared<trace_log_writer>(), trace_level::none));
    ws_transport->on_close(error_callback);

    auto mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();

    client->receive_message(std::make_exception_ptr(std::runtime_error("error")));

    error_event->wait(1000);

    ASSERT_STREQ("error", exception_msg->c_str());
    ASSERT_TRUE(*close_invoked);
}

TEST(websocket_transport_get_transport_type, get_transport_type_returns_websockets)
{
    auto ws_transport = websocket_transport::create(
        [](const signalr_client_config& config)
        {
            auto client = std::make_shared<test_websocket_client>();
            client->set_config(config);
            return client;
        }, signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none));

    ASSERT_EQ(transport_type::websockets, ws_transport->get_transport_type());
}

namespace {
class throwing_websocket_client : public websocket_client
{
public:
    void start(const std::string& url, const std::function<void(const std::exception_ptr&)>& callback) override
    {
        callback(nullptr);
    }

    void stop(const std::function<void(const std::exception_ptr&)>& callback) override
    {
        m_stop = true;
        callback(nullptr);
    }

    void send(
        const std::string& payload, signalr::transfer_format transfer_format,
        const std::function<void(std::exception_ptr)>& callback
    ) override {
        callback(nullptr);
    }

    void receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) override {
        std::thread([callback, this]()
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            std::exception_ptr exception = nullptr;
            if (m_stop == true)
            {
                exception = std::make_exception_ptr(std::runtime_error("throw from receive"));
            }
            callback("", exception);
        }).detach();
    }
private:
    bool m_stop{};
};
}

TEST(stop, error_from_receive_on_stop_properly_closes)
{
    auto ws_transport = websocket_transport::create(
        [](const signalr_client_config& config)
        {
            auto client = std::make_shared<throwing_websocket_client>();
            return client;
        }, signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none));

    std::atomic_int stop_called{ 0 };
    std::atomic_int on_close_called{ 0 };
    auto mre = manual_reset_event<void>();
    ws_transport->on_close([&mre, &on_close_called](const std::exception_ptr& ex)
    {
        ++on_close_called;
        mre.set(ex);
    });
    ws_transport->start("ws://fakeuri.org", [](const std::exception_ptr&) {});
    auto stop_mre = manual_reset_event<void>();
    ws_transport->stop([&stop_mre, &stop_called](const std::exception_ptr& ex)
    {
        ++stop_called;
        stop_mre.set(ex);
    });
    mre.get();
    stop_mre.get();
    ASSERT_EQ(1, on_close_called.load());
    ASSERT_EQ(1, stop_called.load());
}

namespace {
class caching_websocket_client : public websocket_client
{
public:
    void start(const std::string& url,
               const std::function<void(const std::exception_ptr&)>& callback) noexcept override {
        m_start_callback = callback;
        callback(nullptr);
    }

    void stop(const std::function<void(const std::exception_ptr&)>& callback) override
    {
        m_stop_callback = callback;
        callback(nullptr);
    }

    void send(
        const std::string& payload, signalr::transfer_format transfer_format,
        const std::function<void(std::exception_ptr)>& callback
    ) override {
        callback(nullptr);
    }

    void receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) override
    {
        m_receive_callback = callback;
        std::thread([callback]()
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            callback("", nullptr);
        }).detach();
    }
private:
    std::function<void(std::exception_ptr)> m_start_callback;
    std::function<void(std::exception_ptr)> m_stop_callback;
    std::function<void(std::string&&, const std::exception_ptr&)> m_receive_callback;
};
}

// validates that a 3rd party WebSocket client implementation won't cause a memory leak if it caches the callbacks
// passed from the websocket_transport
TEST(websocket_client_custom_impl, caching_callbacks_does_not_leak)
{
    auto ws_transport = websocket_transport::create(
        [](const signalr_client_config& config)
        {
            auto client = std::make_shared<caching_websocket_client>();
            return client;
        },
        signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none, nullptr)
    );

    ws_transport->start("ws://fakeuri.org", [](const std::exception_ptr&) {});
    auto mre = manual_reset_event<void>();
    ws_transport->stop([&mre](const std::exception_ptr& exception)
    {
        mre.set(exception);
    });
    mre.get();
}

// Regression: synchronous callbacks must not run while the transport's state mutex is held.
TEST(websocket_transport_disconnect, stop_when_disconnected_can_start_from_callback) {
    auto client = std::make_shared<test_websocket_client>();
    auto ws_transport = websocket_transport::create(
        [&client](const signalr_client_config& config) {
            client->set_config(config);
            return client;
        },
        signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none)
    );

    auto start_complete = manual_reset_event<void>();
    ws_transport->stop([&](const std::exception_ptr& exception) {
        if (exception) {
            start_complete.set(exception);
            return;
        }

        ws_transport->start("ws://fakeuri.org", [&](const std::exception_ptr& start_error) {
            start_complete.set(start_error);
        });
    });

    start_complete.get();

    auto stop_complete = manual_reset_event<void>();
    ws_transport->stop([&](const std::exception_ptr& exception) {
        stop_complete.set(exception);
    });
    stop_complete.get();
}

// Regression: a retained WebSocket client pointer does not imply a connected transport.
TEST(websocket_transport_send, cannot_send_after_stop) {
    auto client = std::make_shared<test_websocket_client>();
    std::atomic_bool send_called{false};
    client->set_send_function([&](
        const std::string&,
        const std::function<void(std::exception_ptr)>& callback
    ) {
        send_called.store(true);
        callback(nullptr);
    });

    auto ws_transport = websocket_transport::create(
        [&client](const signalr_client_config& config) {
            client->set_config(config);
            return client;
        },
        signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none)
    );

    auto start_complete = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&](const std::exception_ptr& exception) {
        start_complete.set(exception);
    });
    start_complete.get();

    auto stop_complete = manual_reset_event<void>();
    ws_transport->stop([&](const std::exception_ptr& exception) {
        stop_complete.set(exception);
    });
    stop_complete.get();

    auto send_error = std::promise<std::exception_ptr>();
    auto send_result = send_error.get_future();
    ws_transport->send("payload", transfer_format::text, [&](const std::exception_ptr& exception) {
        send_error.set_value(exception);
    });

    ASSERT_NE(nullptr, send_result.get());
    ASSERT_FALSE(send_called.load());
}

// 1. Tests that synchronous start exceptions do not strand pending stop callbacks
TEST(websocket_transport_lifecycle, synchronous_start_failure_drains_stop_callbacks) {
    auto client = std::make_shared<test_websocket_client>();
    client->set_connect_function([](const std::string&, const std::function<void(std::exception_ptr)>&) {
        throw std::runtime_error("immediate start exception");
    });

    auto ws_transport = websocket_transport::create(
        [&client](const signalr_client_config& config) {
            client->set_config(config);
            return client;
        },
        signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none)
    );

    auto start_mre = manual_reset_event<void>();
    auto stop_mre = manual_reset_event<void>();

    ws_transport->start("ws://fakeuri.org", [&](const std::exception_ptr& ex) {
        start_mre.set(ex);
    });

    ws_transport->stop([&](const std::exception_ptr& ex) {
        stop_mre.set(ex);
    });

    try {
        start_mre.get();
        ASSERT_TRUE(false);
    } catch (const std::runtime_error& ex) {
        ASSERT_STREQ("immediate start exception", ex.what());
    }

    // Must not hang: stop callback must execute exactly once
    stop_mre.get();
}

// 2. Tests that an exception in on_close does not prevent stop callbacks from completing
TEST(websocket_transport_lifecycle, throwing_on_close_does_not_strand_stop_callbacks) {
    auto client = std::make_shared<test_websocket_client>();
    auto ws_transport = websocket_transport::create(
        [&client](const signalr_client_config& config) {
            client->set_config(config);
            return client;
        },
        signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none)
    );

    ws_transport->on_close([](const std::exception_ptr&) {
        throw std::runtime_error("user callback explosion");
    });

    auto start_mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&](const std::exception_ptr& ex) {
        start_mre.set(ex);
    });
    start_mre.get();

    auto stop_mre = manual_reset_event<void>();
    ws_transport->stop([&](const std::exception_ptr& ex) {
        stop_mre.set(ex);
    });

    // If on_close throwing aborted execution, this would hang
    stop_mre.get();
}

namespace {

class stale_callback_websocket_client : public websocket_client {
public:
    void start(
        const std::string&,
        const std::function<void(const std::exception_ptr&)>& callback
    ) override {
        callback(nullptr);
    }

    void stop(const std::function<void(const std::exception_ptr&)>& callback) override {
        // Complete the in-flight receive loop with EOF so stop completes cleanly,
        // while intentionally keeping m_receive_callback alive to simulate a stale delayed call later.
        auto cb = m_receive_callback;
        if (cb) {
            cb("", nullptr);
        }
        callback(nullptr);
    }

    void send(
        const std::string&,
        signalr::transfer_format,
        const std::function<void(std::exception_ptr)>& callback
    ) override {
        callback(nullptr);
    }

    void receive(const std::function<void(std::string&&, const std::exception_ptr&)>& callback) override {
        m_receive_callback = callback;
    }

    std::function<void(std::string&&, const std::exception_ptr&)> m_receive_callback;
};

} // namespace

TEST(websocket_transport_lifecycle, previous_session_cannot_pollute_new_session) {
    std::shared_ptr<stale_callback_websocket_client> first_client;
    std::shared_ptr<stale_callback_websocket_client> second_client;
    int client_index = 0;

    auto ws_transport = websocket_transport::create(
        [&](const signalr_client_config&) -> std::shared_ptr<websocket_client> {
            if (++client_index == 1) {
                first_client = std::make_shared<stale_callback_websocket_client>();
                return first_client;
            }
            second_client = std::make_shared<stale_callback_websocket_client>();
            return second_client;
        },
        signalr_client_config{},
        logger(std::make_shared<trace_log_writer>(), trace_level::none)
    );

    int close_event_count = 0;
    ws_transport->on_close([&](const std::exception_ptr&) {
        close_event_count++;
    });

    // Start session 1
    auto start1_mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&](const std::exception_ptr& ex) {
        start1_mre.set(ex);
    });
    start1_mre.get();

    // Stop session 1
    auto stop1_mre = manual_reset_event<void>();
    ws_transport->stop([&](const std::exception_ptr& ex) {
        stop1_mre.set(ex);
    });
    stop1_mre.get();

    ASSERT_EQ(1, close_event_count);

    // Start session 2
    auto start2_mre = manual_reset_event<void>();
    ws_transport->start("ws://fakeuri.org", [&](const std::exception_ptr& ex) {
        start2_mre.set(ex);
    });
    start2_mre.get();

    // Invoke the stale receive callback from session 1 while session 2 is active
    ASSERT_TRUE(first_client->m_receive_callback != nullptr);
    first_client->m_receive_callback(
        "",
        std::make_exception_ptr(std::runtime_error("stale error from past connection"))
    );

    // Verify session 2 ignored the stale error and close_event_count is unchanged
    ASSERT_EQ(1, close_event_count);

    // Stop session 2
    auto stop2_mre = manual_reset_event<void>();
    ws_transport->stop([&](const std::exception_ptr& ex) {
        stop2_mre.set(ex);
    });
    stop2_mre.get();

    ASSERT_EQ(2, close_event_count);
}

namespace websocket_transport_lifecycle_additional {

using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    // This fake completes start immediately by default, but deliberately holds stop
    // and receive callbacks. Tests choose the exact order of asynchronous events.
    class controlled_websocket_client final : public signalr::websocket_client {
    public:
        explicit controlled_websocket_client(bool delay_start = false) : m_delay_start(delay_start) {}

        void start(const std::string&, const completion_callback& callback) override {
            completion_callback completion;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                ++m_start_calls;
                if (m_delay_start) {
                    m_start_callback = callback;
                } else {
                    completion = callback;
                }
            }
            if (completion) {
                completion(nullptr);
            }
        }

        void stop(const completion_callback& callback) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_stop_calls;
            m_stop_callback = callback;
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            callback(nullptr);
        }

        void receive(const receive_callback& callback) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_receive_calls;
            m_receive_callback = callback;
        }

        void complete_start(const std::exception_ptr& exception = nullptr) {
            completion_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = std::move(m_start_callback);
            }
            if (callback) {
                callback(exception);
            }
        }

        void complete_stop(const std::exception_ptr& exception = nullptr) {
            completion_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = std::move(m_stop_callback);
            }
            if (callback) {
                callback(exception);
            }
        }

        void complete_receive(std::string message = {}, const std::exception_ptr& exception = nullptr) {
            receive_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = std::move(m_receive_callback);
            }
            if (callback) {
                callback(std::move(message), exception);
            }
        }

        int stop_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_stop_calls;
        }

    private:
        mutable std::mutex m_mutex;
        bool m_delay_start;
        int m_start_calls{0};
        int m_stop_calls{0};
        int m_receive_calls{0};
        completion_callback m_start_callback;
        completion_callback m_stop_callback;
        receive_callback m_receive_callback;
    };
}

static std::shared_ptr<signalr::transport> make_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );
}

// Normal stop is deliberately completed in two steps: the underlying stop
// callback first, then the outstanding receive callback. This tests that
// completion waits for BOTH operations rather than assuming stop is immediate.
static void finish_connected_stop(const std::shared_ptr<controlled_websocket_client>& client) {
    client->complete_stop();
    client->complete_receive();
}

TEST(websocket_transport_lifecycle_additional, second_stop_waits_for_first_shutdown) {
    auto client = std::make_shared<controlled_websocket_client>();
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });

    int start_callbacks = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& exception) {
        if (!exception) {
            ++start_callbacks;
        }
    });
    ASSERT_EQ(1, start_callbacks);

    int first_stop_callbacks = 0;
    int second_stop_callbacks = 0;
    transport->stop([&](const std::exception_ptr&) { ++first_stop_callbacks; });
    ASSERT_EQ(1, client->stop_calls());
    ASSERT_EQ(0, first_stop_callbacks);

    transport->stop([&](const std::exception_ptr&) { ++second_stop_callbacks; });
    // Both requests must remain pending while the underlying close is unfinished.
    EXPECT_EQ(0, second_stop_callbacks);

    finish_connected_stop(client);
    EXPECT_EQ(1, first_stop_callbacks);
    EXPECT_EQ(1, second_stop_callbacks);
    EXPECT_EQ(1, client->stop_calls());
}

TEST(websocket_transport_lifecycle_additional, cannot_replace_session_while_stop_is_pending) {
    auto first = std::make_shared<controlled_websocket_client>();
    std::shared_ptr<controlled_websocket_client> second;
    int creations = 0;

    auto transport = make_transport([&](const signalr::signalr_client_config&)
        -> std::shared_ptr<signalr::websocket_client> {
        if (++creations == 1) {
            return first;
        }
        second = std::make_shared<controlled_websocket_client>();
        return second;
    });

    int start1_callbacks = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr&) { ++start1_callbacks; });
    ASSERT_EQ(1, start1_callbacks);

    int stop1_callbacks = 0;
    transport->stop([&](const std::exception_ptr&) { ++stop1_callbacks; });
    ASSERT_EQ(0, stop1_callbacks);

    int start2_callbacks = 0;
    std::exception_ptr start2_error;
    transport->start("ws://example.test", [&](const std::exception_ptr& exception) {
        ++start2_callbacks;
        start2_error = exception;
    });

    // Finish both sessions before asserting, even if the implementation incorrectly
    // allowed the second start. This avoids leaving pending asynchronous callbacks.
    finish_connected_stop(first);
    if (second) {
        transport->stop([](const std::exception_ptr&) {});
        finish_connected_stop(second);
    }

    EXPECT_EQ(1, start2_callbacks);
    EXPECT_NE(nullptr, start2_error) << "start() should reject a still-disconnecting transport";
    EXPECT_EQ(1, stop1_callbacks) << "replacing the session must not abandon the first stop callback";
    EXPECT_EQ(1, creations) << "a rejected start should not construct a second WebSocket client";
}

TEST(websocket_transport_lifecycle_additional, rejected_start_callback_does_not_hold_transport_mutex) {
    auto client = std::make_shared<controlled_websocket_client>();
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    std::promise<void> reentry_completed;
    auto reentry_future = reentry_completed.get_future();
    std::thread reentry_worker;
    bool rejected_callback_called = false;
    bool callback_was_lock_free = false;

    transport->start("ws://example.test", [&](const std::exception_ptr& exception) {
        rejected_callback_called = (exception != nullptr);
        reentry_worker = std::thread([transport, &reentry_completed]() {
            transport->on_receive([](std::string&&, const std::exception_ptr&) {});
            reentry_completed.set_value();
        });
        // This wait is bounded. On the buggy implementation it times out, the
        // callback returns, and the blocked worker can finish and be joined.
        callback_was_lock_free =
            reentry_future.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
    });

    if (reentry_worker.joinable()) {
        reentry_worker.join();
    }
    transport->stop([](const std::exception_ptr&) {});
    finish_connected_stop(client);

    EXPECT_TRUE(rejected_callback_called);
    EXPECT_TRUE(callback_was_lock_free) << "rejected start invoked a callback while m_mutex was locked";
}

TEST(websocket_transport_lifecycle_additional, aborted_connect_propagates_websocket_stop_error) {
    auto client = std::make_shared<controlled_websocket_client>(true);
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });

    std::exception_ptr start_error;
    std::exception_ptr stop_error;
    int start_count = 0;
    int stop_count = 0;

    transport->start("ws://example.test", [&](const std::exception_ptr& exception) {
        ++start_count;
        start_error = exception;
    });
    transport->stop([&](const std::exception_ptr& exception) {
        ++stop_count;
        stop_error = exception;
    });

    client->complete_start();
    ASSERT_EQ(1, client->stop_calls());
    client->complete_stop(std::make_exception_ptr(std::runtime_error("websocket stop failed")));

    EXPECT_EQ(1, start_count);
    EXPECT_NE(nullptr, start_error);  // Starting was canceled.
    EXPECT_EQ(1, stop_count);
    EXPECT_NE(nullptr, stop_error) << "aborted connection discarded the underlying stop error";
}

TEST(websocket_transport_lifecycle_additional, send_callback_exception_is_not_delivered_twice) {
    auto client = std::make_shared<controlled_websocket_client>();
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    int callback_count = 0;
    transport->send("hello", signalr::transfer_format::text, [&](const std::exception_ptr&) {
        if (++callback_count == 1) {
            throw std::runtime_error("user callback exception");
        }
    });

    transport->stop([](const std::exception_ptr&) {});
    finish_connected_stop(client);

    EXPECT_EQ(1, callback_count) << "a throwing user callback must not be invoked a second time";
}

TEST(websocket_transport_lifecycle_additional, start_callback_exception_is_not_delivered_twice) {
    auto client = std::make_shared<controlled_websocket_client>();
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });

    int callback_count = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr&) {
        if (++callback_count == 1) {
            throw std::runtime_error("user start callback exception");
        }
    });

    // A failed assertion must not leave a live client waiting for a shutdown event.
    transport->stop([](const std::exception_ptr&) {});
    finish_connected_stop(client);
    EXPECT_EQ(1, callback_count) << "start() interpreted a throwing user callback as another connection failure";
}

} // websocket_transport_lifecycle_additional


namespace websocket_transport_round4 {

using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class controlled_websocket_client final : public signalr::websocket_client {
    public:
        enum class start_mode {
            succeed,
            fail,
            deferred
        };

        explicit controlled_websocket_client(start_mode mode = start_mode::succeed) : m_mode(mode) {}

        void start(const std::string&, const completion_callback& callback) override {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                ++m_start_calls;
                m_start_callback = callback;
            }

            if (m_mode == start_mode::succeed) {
                callback(nullptr);
            } else if (m_mode == start_mode::fail) {
                callback(std::make_exception_ptr(std::runtime_error("start failed")));
            }
        }

        void stop(const completion_callback& callback) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_stop_calls;
            if (m_throw_on_stop) {
                throw std::runtime_error("client stop threw");
            }
            m_stop_callback = callback;
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                ++m_send_calls;
            }
            callback(nullptr);
        }

        void receive(const receive_callback& callback) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_receive_calls;
            m_receive_callbacks.push_back(callback);
        }

        void set_throw_on_stop(bool value) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_throw_on_stop = value;
        }

        // Deliberately retains the callback to emulate a misbehaving implementation
        // that reports start completion more than once.
        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_start(const std::exception_ptr& error = nullptr) {
            completion_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_start_callback;
            }
            if (callback) {
                callback(error);
            }
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_stop(const std::exception_ptr& error = nullptr) {
            completion_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_stop_callback;
            }
            if (callback) {
                callback(error);
            }
        }

        void emit_receive(const std::exception_ptr& error = nullptr, std::string payload = {}) {
            receive_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_receive_callbacks.empty()) {
                    return;
                }
                callback = std::move(m_receive_callbacks.front());
                m_receive_callbacks.erase(m_receive_callbacks.begin());
            }
            callback(std::move(payload), error);
        }

        // ReSharper disable once CppDeclaratorNeverUsed
        int start_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_start_calls;
        }

        int stop_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_stop_calls;
        }

        int receive_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_receive_calls;
        }

        int send_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_send_calls;
        }

    private:
        const start_mode m_mode;
        mutable std::mutex m_mutex;
        bool m_throw_on_stop{false};
        int m_start_calls{0};
        int m_stop_calls{0};
        int m_receive_calls{0};
        int m_send_calls{0};
        completion_callback m_start_callback;
        completion_callback m_stop_callback;
        std::vector<receive_callback> m_receive_callbacks;
    };
}

static std::shared_ptr<signalr::transport> make_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );
}

// A connected session's normal stop completes only after BOTH client.stop()
// and the outstanding client.receive() callbacks have run.
static void finish_stop(const std::shared_ptr<controlled_websocket_client>& client) {
    client->emit_stop();
    client->emit_receive();
}

TEST(websocket_transport_round4, failed_start_callback_can_retry_immediately) {
    auto failed_client = std::make_shared<controlled_websocket_client>(
        controlled_websocket_client::start_mode::fail
    );
    auto good_client = std::make_shared<controlled_websocket_client>();
    int factory_calls = 0;

    auto transport = make_transport([&](const signalr::signalr_client_config&)
        -> std::shared_ptr<signalr::websocket_client> {
        return ++factory_calls == 1 ? failed_client : good_client;
    });

    std::exception_ptr first_error;
    std::exception_ptr retry_error;
    int retry_callbacks = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        first_error = error;
        transport->start("ws://example.test", [&](const std::exception_ptr& retry) {
            retry_error = retry;
            ++retry_callbacks;
        });
    });

    EXPECT_NE(nullptr, first_error);
    EXPECT_EQ(1, retry_callbacks);
    EXPECT_EQ(nullptr, retry_error) << "A failed-start callback should observe the transport as stopped";
    EXPECT_EQ(2, factory_calls);

    // Clean up even when this regression fails.
    transport->stop([](const std::exception_ptr&) {});
    finish_stop(good_client);
}

TEST(websocket_transport_round4, duplicate_successful_start_does_not_start_another_receive) {
    auto client = std::make_shared<controlled_websocket_client>(
        controlled_websocket_client::start_mode::deferred
    );
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });

    int start_callbacks = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++start_callbacks;
    });

    client->emit_start();
    ASSERT_EQ(1, client->receive_calls());
    client->emit_start(); // The same completion delivered a second time.

    EXPECT_EQ(1, start_callbacks);
    EXPECT_EQ(1, client->receive_calls()) << "A duplicate start completion launched a second receive loop";

    transport->stop([](const std::exception_ptr&) {});
    finish_stop(client);
}

TEST(websocket_transport_round4, stale_start_error_does_not_disconnect_a_live_session) {
    auto client = std::make_shared<controlled_websocket_client>(
        controlled_websocket_client::start_mode::deferred
    );
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });

    int start_callbacks = 0;
    int close_notifications = 0;
    transport->on_close([&](const std::exception_ptr&) { ++close_notifications; });
    transport->start("ws://example.test", [&](const std::exception_ptr&) { ++start_callbacks; });
    client->emit_start();
    ASSERT_EQ(1, client->receive_calls());

    client->emit_start(std::make_exception_ptr(std::runtime_error("late start error")));
    std::exception_ptr send_error;
    transport->send("still alive", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        send_error = error;
    });

    EXPECT_EQ(1, start_callbacks);
    EXPECT_EQ(0, close_notifications) << "A stale start error closed an already-connected session";
    EXPECT_EQ(nullptr, send_error) << "A stale start error prevented sending on the live connection";
    EXPECT_EQ(1, client->send_calls());

    transport->stop([](const std::exception_ptr&) {});
    finish_stop(client);
}

TEST(websocket_transport_round4, receive_error_with_throwing_client_stop_completes_shutdown) {
    auto client = std::make_shared<controlled_websocket_client>();
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });
    client->set_throw_on_stop(true);

    int close_notifications = 0;
    std::exception_ptr close_error;
    transport->on_close([&](const std::exception_ptr& error) {
        ++close_notifications;
        close_error = error;
    });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    bool escaped_receive_callback = false;
    try {
        client->emit_receive(std::make_exception_ptr(std::runtime_error("receive failed")));
    } catch (...) {
        escaped_receive_callback = true;
    }

    // The original receive error should be reported, even if cleanup also fails.
    EXPECT_FALSE(escaped_receive_callback) << "The receive callback leaked an exception from client.stop()";
    EXPECT_EQ(1, close_notifications) << "The session never finalized after client.stop() threw";
    EXPECT_NE(nullptr, close_error);

    // This revision may leave the session disconnecting; its destructor uses
    // a bounded shutdown wait. No additional blocking wait is performed here.
}

TEST(websocket_transport_round4, aborted_connect_with_throwing_client_stop_drains_callbacks) {
    auto client = std::make_shared<controlled_websocket_client>(
        controlled_websocket_client::start_mode::deferred
    );
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });
    client->set_throw_on_stop(true);

    int start_callbacks = 0;
    int stop_callbacks = 0;
    std::exception_ptr start_error;
    std::exception_ptr stop_error;

    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        ++start_callbacks;
        start_error = error;
    });
    transport->stop([&](const std::exception_ptr& error) {
        ++stop_callbacks;
        stop_error = error;
    });

    bool escaped_start_callback = false;
    try {
        client->emit_start();
    } catch (...) {
        escaped_start_callback = true;
    }

    EXPECT_FALSE(escaped_start_callback) << "The start-completion callback leaked an exception from client.stop()";
    EXPECT_EQ(1, start_callbacks);
    EXPECT_NE(nullptr, start_error);
    EXPECT_EQ(1, stop_callbacks);
    EXPECT_NE(nullptr, stop_error);
}

TEST(websocket_transport_round4, duplicate_websocket_stop_completion_is_ignored) {
    auto client = std::make_shared<controlled_websocket_client>();
    auto transport = make_transport([client](const signalr::signalr_client_config&) { return client; });

    int stop_callbacks = 0;
    int close_notifications = 0;
    transport->on_close([&](const std::exception_ptr&) { ++close_notifications; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    transport->stop([&](const std::exception_ptr&) { ++stop_callbacks; });
    client->emit_stop();
    client->emit_receive();
    client->emit_stop();

    EXPECT_EQ(1, stop_callbacks);
    EXPECT_EQ(1, close_notifications);
    EXPECT_EQ(1, client->stop_calls());
}

TEST(websocket_transport_round4, mock_client_delivers_two_messages_in_order) {
    auto client = std::make_shared<signalr::test_websocket_client>();
    client->set_config(signalr::signalr_client_config{});

    std::promise<std::exception_ptr> started;
    auto start_result = started.get_future();
    client->start("ws://example.test", [&](const std::exception_ptr& error) { started.set_value(error); });
    ASSERT_EQ(std::future_status::ready, start_result.wait_for(std::chrono::seconds(3)));
    ASSERT_EQ(nullptr, start_result.get());

    std::mutex received_mutex;
    std::vector<std::string> received;
    std::promise<void> second_received;
    auto second_result = second_received.get_future();

    client->receive([&](std::string&& first, const std::exception_ptr& error) {
        if (error) {
            second_received.set_value();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(received_mutex);
            received.push_back(std::move(first));
        }
        client->receive([&](std::string&& second, const std::exception_ptr& next_error) {
            if (!next_error) {
                std::lock_guard<std::mutex> lock(received_mutex);
                received.push_back(std::move(second));
            }
            second_received.set_value();
        });
    });

    client->receive_message("one");
    client->receive_message("two");
    ASSERT_EQ(std::future_status::ready, second_result.wait_for(std::chrono::seconds(3)));
    second_result.get();

    {
        std::lock_guard<std::mutex> lock(received_mutex);
        ASSERT_EQ(2U, received.size());
        EXPECT_EQ("one", received[0]);
        EXPECT_EQ("two", received[1]);
    }
    EXPECT_EQ(2, client->receive_count.load());

    std::promise<std::exception_ptr> stopped;
    auto stop_result = stopped.get_future();
    client->stop([&](const std::exception_ptr& error) { stopped.set_value(error); });
    ASSERT_EQ(std::future_status::ready, stop_result.wait_for(std::chrono::seconds(3)));
    EXPECT_EQ(nullptr, stop_result.get());
}

} // websocket_transport_round4

namespace websocket_transport_round5
{
using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class round5_websocket_client final : public signalr::websocket_client {
    public:
        void start(const std::string&, const completion_callback& callback) override {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_defer_start) {
                    m_start_callback = callback;
                    return;
                }
            }
            callback(nullptr);
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_start(const std::exception_ptr& error = nullptr) {
            completion_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_start_callback;
            }
            if (callback) {
                callback(error);
            }
        }

        void set_defer_start(bool value) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_defer_start = value;
        }

        void stop(const completion_callback& callback) override {
            receive_callback receive;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                ++m_stop_calls;
                m_last_stop_callback = callback;
                if (m_defer_stop) {
                    return;
                }
                if (m_complete_receive_on_stop) {
                    receive = std::move(m_receive_callback);
                }
            }

            if (receive) {
                receive("", nullptr);
            }
            callback(nullptr);
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            ++m_send_calls;
            callback(nullptr);
        }

        void receive(const receive_callback& callback) override {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_receive_calls;
            if (m_throw_on_receive) {
                throw std::runtime_error("receive registration failed");
            }
            m_receive_callback = callback;
        }

        void set_defer_stop(bool value) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_defer_stop = value;
        }

        void set_complete_receive_on_stop(bool value) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_complete_receive_on_stop = value;
        }

        void set_throw_on_receive(bool value) {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_throw_on_receive = value;
        }

        void emit_receive(std::string message = {}, const std::exception_ptr& error = nullptr) {
            receive_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = std::move(m_receive_callback);
                m_last_receive_callback = callback;
            }
            if (callback) {
                callback(std::move(message), error);
            }
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void repeat_last_receive(std::string message) {
            receive_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_last_receive_callback;
            }
            if (callback) {
                callback(std::move(message), nullptr);
            }
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_stop(const std::exception_ptr& error = nullptr) {
            completion_callback callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_last_stop_callback;
            }
            if (callback) {
                callback(error);
            }
        }

        int receive_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_receive_calls;
        }

        int stop_calls() const {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_stop_calls;
        }

        int send_calls() const {
            return m_send_calls.load();
        }

    private:
        mutable std::mutex m_mutex;
        bool m_defer_start{false};
        bool m_defer_stop{false};
        bool m_complete_receive_on_stop{true};
        bool m_throw_on_receive{false};
        int m_receive_calls{0};
        int m_stop_calls{0};
        std::atomic<int> m_send_calls{0};
        receive_callback m_receive_callback;
        receive_callback m_last_receive_callback;
        completion_callback m_last_stop_callback;
        completion_callback m_start_callback;
    };
}

static std::shared_ptr<signalr::transport> make_round5_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );
}

// Regression: an inline client.send completion must not prevent another thread from stopping the transport.
// The wait is bounded. The worker is joined *after* send() returns, so the buggy implementation does not hang tests.
TEST(websocket_transport_round5, synchronous_send_callback_does_not_hold_session_mutex) {
    auto client = std::make_shared<round5_websocket_client>();
    auto transport = make_round5_transport([client](const signalr::signalr_client_config&) { return client; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    std::promise<void> stopped;
    auto stopped_future = stopped.get_future();
    std::thread stop_thread;
    bool stop_finished_inside_send_callback = false;

    transport->send("message", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        stop_thread = std::thread([&]() {
            transport->stop([&](const std::exception_ptr& stop_error) {
                EXPECT_EQ(nullptr, stop_error);
                stopped.set_value();
            });
        });
        stop_finished_inside_send_callback =
            stopped_future.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    });

    if (stop_thread.joinable()) {
        stop_thread.join();
    }
    EXPECT_TRUE(stop_finished_inside_send_callback)
        << "client.send invoked its completion while the session mutex was held";
}

// Regression: a non-std exception contained in exception_ptr must still complete all stop requests.
// The test deliberately delays client.stop() completion until the receive token has a registered callback.
TEST(websocket_transport_round5, non_std_websocket_stop_error_completes_stop) {
    auto client = std::make_shared<round5_websocket_client>();
    client->set_defer_stop(true);
    auto transport = make_round5_transport([client](const signalr::signalr_client_config&) { return client; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    int callbacks = 0;
    std::exception_ptr delivered_error;
    transport->stop([&](const std::exception_ptr& error) {
        ++callbacks;
        delivered_error = error;
    });
    client->emit_stop(std::make_exception_ptr(42));

    bool escaped_callback = false;
    try {
        client->emit_receive();
    } catch (...) {
        escaped_callback = true;
    }

    EXPECT_FALSE(escaped_callback);
    EXPECT_EQ(1, callbacks);
    EXPECT_NE(nullptr, delivered_error);

    // Recover the current implementation if it did not complete on the first stop result.
    if (callbacks == 0) {
        client->emit_stop();
    }
}

// If WebSocket opening succeeds but registering the first receive throws, the connected client must be closed.
TEST(websocket_transport_round5, failed_first_receive_registration_closes_open_websocket) {
    auto client = std::make_shared<round5_websocket_client>();
    client->set_throw_on_receive(true);
    auto transport = make_round5_transport([client](const signalr::signalr_client_config&) { return client; });

    int start_callbacks = 0;
    std::exception_ptr start_error;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        ++start_callbacks;
        start_error = error;
    });

    EXPECT_EQ(1, start_callbacks);
    EXPECT_NE(nullptr, start_error);
    EXPECT_EQ(1, client->stop_calls())
        << "The transport abandoned a successfully opened WebSocket after receive() threw";
}

// Defensive contract test: one receive registration must deliver no more than one message.
TEST(websocket_transport_round5, duplicate_receive_callback_is_ignored) {
    auto client = std::make_shared<round5_websocket_client>();
    auto transport = make_round5_transport([client](const signalr::signalr_client_config&) { return client; });
    int messages = 0;
    transport->on_receive([&](std::string&&, const std::exception_ptr&) { ++messages; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    client->emit_receive("first");
    client->repeat_last_receive("duplicate");

    EXPECT_EQ(1, messages);
    EXPECT_EQ(2, client->receive_calls())
        << "Repeating one completed receive should not schedule an additional receive";

    transport->stop([](const std::exception_ptr&) {});
}

// Positive regression: a close handler may start a new connection, and a late callback from the old
// client's stop must not clear or disrupt that new connection.
TEST(websocket_transport_round5, close_callback_can_reconnect_and_ignore_old_stop_completion) {
    auto first = std::make_shared<round5_websocket_client>();
    auto second = std::make_shared<round5_websocket_client>();
    int factory_calls = 0;
    auto transport = make_round5_transport([&](const signalr::signalr_client_config&)
        -> std::shared_ptr<signalr::websocket_client> {
        return ++factory_calls == 1 ? first : second;
    });

    int close_callbacks = 0;
    int retry_callbacks = 0;
    transport->on_close([&](const std::exception_ptr&) {
        ++close_callbacks;
        if (close_callbacks == 1) {
            transport->start("ws://example.test/reconnected", [&](const std::exception_ptr& error) {
                EXPECT_EQ(nullptr, error);
                ++retry_callbacks;
            });
        }
    });

    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    int stop_callbacks = 0;
    transport->stop([&](const std::exception_ptr&) { ++stop_callbacks; });
    first->emit_stop(); // Old WebSocket client violates the contract and completes stop again.

    int sends = 0;
    transport->send("live", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++sends;
    });

    EXPECT_EQ(2, factory_calls);
    EXPECT_EQ(1, retry_callbacks);
    EXPECT_EQ(1, stop_callbacks);
    EXPECT_EQ(1, sends);
    EXPECT_EQ(1, second->send_calls());

    transport->on_close([](const std::exception_ptr&) {});
    transport->stop([](const std::exception_ptr&) {});
}

// Contract-dependent test: if WebSocket stop completes without resolving a pending receive,
// should transport.stop() still complete? See section 3.8 of REVIEW.md.
TEST(websocket_transport_round5, stop_completes_without_receive_callback) {
    auto client = std::make_shared<round5_websocket_client>();
    client->set_complete_receive_on_stop(false);
    auto transport = make_round5_transport([client](const signalr::signalr_client_config&) { return client; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    int stop_callbacks = 0;
    transport->stop([&](const std::exception_ptr&) { ++stop_callbacks; });
    EXPECT_EQ(1, stop_callbacks)
        << "Stop requires the underlying client to resolve its pending receive; this must be documented or fixed";

    // Always finish the outstanding receive to avoid delaying teardown in the current implementation.
    client->emit_receive();
}

// Contract-dependent stack-safety test: a client may complete receives inline when data is buffered.
// The test uses a finite burst, so the current recursive implementation fails without exhausting the stack.
namespace {

    class round5_inline_receive_client final : public signalr::websocket_client {
    public:
        explicit round5_inline_receive_client(int messages) : m_remaining(messages) {}

        void start(const std::string&, const completion_callback& callback) override {
            callback(nullptr);
        }

        void stop(const completion_callback& callback) override {
            if (m_pending_receive) {
                auto pending = std::move(m_pending_receive);
                pending("", nullptr);
            }
            callback(nullptr);
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            callback(nullptr);
        }

        void receive(const receive_callback& callback) override {
            ++m_depth;
            if (m_depth > m_max_depth) {
                m_max_depth = m_depth;
            }

            if (m_remaining > 0) {
                --m_remaining;
                callback("buffered message", nullptr);
            } else {
                m_pending_receive = callback;
            }
            --m_depth;
        }

        int max_receive_depth() const {
            return m_max_depth;
        }

    private:
        int m_remaining;
        int m_depth{0};
        int m_max_depth{0};
        receive_callback m_pending_receive;
    };

} // namespace

TEST(websocket_transport_round5, buffered_inline_receives_do_not_grow_the_stack) {
    constexpr int message_count = 128;
    auto client = std::make_shared<round5_inline_receive_client>(message_count);
    auto transport = make_round5_transport([client](const signalr::signalr_client_config&) { return client; });

    int delivered_messages = 0;
    transport->on_receive([&](std::string&&, const std::exception_ptr&) {
        ++delivered_messages;
    });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    EXPECT_EQ(message_count, delivered_messages);
    EXPECT_TRUE(client->max_receive_depth() <= 4)
        << "Inline receive completions recursively called receive_loop()";

    transport->stop([](const std::exception_ptr&) {});
}

// Retaining a close callback must not form a cycle: session -> client -> cached callback -> session.
TEST(websocket_transport_round5, receive_error_stop_callback_does_not_leak_session) {
    std::weak_ptr<round5_websocket_client> weak_client;
    {
        auto client = std::make_shared<round5_websocket_client>();
        weak_client = client;
        auto transport = make_round5_transport([client](const signalr::signalr_client_config&) {
            return client;
        });
        transport->start("ws://example.test", [](const std::exception_ptr&) {});
        client->emit_receive("", std::make_exception_ptr(std::runtime_error("connection dropped")));
    }

    EXPECT_TRUE(weak_client.expired())
        << "A cached client.stop callback retained the session, which retained the client";
}

// The canceled-start shutdown path must also avoid strong captures of the session in a cached stop callback.
TEST(websocket_transport_round5, aborted_start_stop_callback_does_not_leak_session) {
    std::weak_ptr<round5_websocket_client> weak_client;
    {
        auto client = std::make_shared<round5_websocket_client>();
        client->set_defer_start(true);
        weak_client = client;
        auto transport = make_round5_transport([client](const signalr::signalr_client_config&) {
            return client;
        });
        transport->start("ws://example.test", [](const std::exception_ptr&) {});
        transport->stop([](const std::exception_ptr&) {});
        client->emit_start();
    }

    EXPECT_TRUE(weak_client.expired())
        << "Canceled-start cleanup cached a callback that strongly retained the session";
}

} // namespace websocket_transport_round5

namespace websocket_transport_round6 {

using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class round6_websocket_client final : public signalr::websocket_client {
    public:
        bool defer_start{false};
        bool defer_stop{false};
        bool throw_after_start_completion{false};
        int throw_on_receive_number{0};

        int start_calls{0};
        int stop_calls{0};
        int receive_calls{0};
        int send_calls{0};

        void start(const std::string&, const completion_callback& callback) override {
            ++start_calls;
            m_start_callback = callback;
            if (defer_start) {
                return;
            }
            callback(nullptr);
            if (throw_after_start_completion) {
                throw std::runtime_error("start threw after completion");
            }
        }

        void stop(const completion_callback& callback) override {
            ++stop_calls;
            m_stop_callback = callback;
            if (!defer_stop) {
                callback(nullptr);
            }
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            ++send_calls;
            callback(nullptr);
        }

        void receive(const receive_callback& callback) override {
            ++receive_calls;
            if (throw_on_receive_number > 0 && receive_calls == throw_on_receive_number) {
                throw std::runtime_error("receive registration failed");
            }
            m_receive_callback = callback;
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_start(const std::exception_ptr& error = nullptr) {
            if (m_start_callback) {
                m_start_callback(error);
            }
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_stop(const std::exception_ptr& error = nullptr) {
            if (m_stop_callback) {
                m_stop_callback(error);
            }
        }

        void emit_receive(const std::string& message, const std::exception_ptr& error = nullptr) {
            auto callback = std::move(m_receive_callback);
            m_receive_callback = nullptr;
            if (callback) {
                callback(std::string(message), error);
            }
        }

    private:
        completion_callback m_start_callback;
        completion_callback m_stop_callback;
        receive_callback m_receive_callback;
    };
}

static std::shared_ptr<signalr::transport> make_round6_transport(const std::shared_ptr<round6_websocket_client>& client) {
    return signalr::websocket_transport::create(
        [client](const signalr::signalr_client_config&) { return client; },
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );
}

// A later receive-registration failure happens on the backend's callback thread, not in the initial start() call.
// It must be handled without leaking an exception across that boundary or leaving the transport connected.
TEST(websocket_transport_round6, second_receive_registration_failure_closes_connection) {
    auto client = std::make_shared<round6_websocket_client>();
    client->throw_on_receive_number = 2;
    auto transport = make_round6_transport(client);

    int start_callbacks = 0;
    int close_callbacks = 0;
    int received_messages = 0;
    transport->on_close([&](const std::exception_ptr& error) {
        ++close_callbacks;
        EXPECT_NE(nullptr, error);
    });
    transport->on_receive([&](std::string&&, const std::exception_ptr&) { ++received_messages; });
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++start_callbacks;
    });

    EXPECT_EQ(1, client->receive_calls);
    EXPECT_NO_THROW(client->emit_receive("first"));
    EXPECT_EQ(1, start_callbacks);
    EXPECT_EQ(1, received_messages);
    EXPECT_EQ(1, client->stop_calls);
    EXPECT_EQ(1, close_callbacks);

    // Cleanup is still valid when the current implementation has not automatically stopped.
    transport->stop([](const std::exception_ptr&) {});
}

// A duplicate successful start callback while an aborted connection is still stopping must not request stop twice.
TEST(websocket_transport_round6, repeated_successful_connect_completion_stops_client_once) {
    auto client = std::make_shared<round6_websocket_client>();
    client->defer_start = true;
    client->defer_stop = true;
    auto transport = make_round6_transport(client);

    int start_callbacks = 0;
    int stop_callbacks = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++start_callbacks;
    });
    transport->stop([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++stop_callbacks;
    });

    client->emit_start();
    client->emit_start();
    EXPECT_EQ(1, client->stop_calls);
    EXPECT_EQ(1, start_callbacks);

    client->emit_stop();
    EXPECT_EQ(1, stop_callbacks);
}

// The transport may be destroyed while a prior explicit stop is awaiting backend completion.
// A callback accepted by transport.stop() should not disappear solely because the transport was released.
TEST(websocket_transport_round6, pending_stop_completion_survives_transport_destruction) {
    auto client = std::make_shared<round6_websocket_client>();
    client->defer_stop = true;
    int completed_stops = 0;

    {
        auto transport = make_round6_transport(client);
        transport->start("ws://example.test", [](const std::exception_ptr&) {});
        transport->stop([&](const std::exception_ptr& error) {
            EXPECT_EQ(nullptr, error);
            ++completed_stops;
        });
        EXPECT_EQ(0, completed_stops);
    }

    client->emit_stop();
    EXPECT_EQ(1, completed_stops);
}

// Defensive backend contract: once it has reported connection success, a subsequent synchronous throw from
// start() must not silently turn a successful transport into a disconnected one without closing the socket.
TEST(websocket_transport_round6, backend_throw_after_start_success_does_not_abandon_connection) {
    auto client = std::make_shared<round6_websocket_client>();
    client->throw_after_start_completion = true;
    auto transport = make_round6_transport(client);

    int starts = 0;
    int sends = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++starts;
    });
    transport->send("payload", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++sends;
    });

    EXPECT_EQ(1, starts);
    EXPECT_EQ(1, sends);
    EXPECT_EQ(1, client->send_calls);
    transport->stop([](const std::exception_ptr&) {});
}

// Positive regression retained: a synchronous send completion can safely reenter stop().
TEST(websocket_transport_round6, inline_send_completion_can_stop_transport) {
    auto client = std::make_shared<round6_websocket_client>();
    auto transport = make_round6_transport(client);
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    int send_callbacks = 0;
    int stop_callbacks = 0;
    transport->send("payload", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++send_callbacks;
        transport->stop([&](const std::exception_ptr& stop_error) {
            EXPECT_EQ(nullptr, stop_error);
            ++stop_callbacks;
        });
    });

    EXPECT_EQ(1, send_callbacks);
    EXPECT_EQ(1, stop_callbacks);
    EXPECT_EQ(1, client->stop_calls);
}

// Positive regression: a normal stop need not wait for the underlying client to complete a pending receive.
TEST(websocket_transport_round6, stop_completes_without_pending_receive_callback) {
    auto client = std::make_shared<round6_websocket_client>();
    auto transport = make_round6_transport(client);
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    int stop_callbacks = 0;
    transport->stop([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++stop_callbacks;
    });

    EXPECT_EQ(1, stop_callbacks);
    EXPECT_EQ(1, client->stop_calls);
}

} // namespace websocket_transport_round6

namespace websocket_transport_round7 {

using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class controlled_round7_websocket_client final : public signalr::websocket_client {
    public:
        bool defer_start{false};
        bool defer_stop{false};
        bool throw_after_start_success{false};

        std::atomic<int> start_calls{0};
        std::atomic<int> stop_calls{0};
        std::atomic<int> receive_calls{0};
        std::atomic<int> send_calls{0};

        void start(const std::string&, const completion_callback& callback) override {
            ++start_calls;
            m_start_callback = callback;
            if (!defer_start) {
                callback(nullptr);
                if (throw_after_start_success) {
                    throw std::runtime_error("backend threw after reporting start success");
                }
            }
        }

        void stop(const completion_callback& callback) override {
            ++stop_calls;
            m_stop_callback = callback;
            if (!defer_stop) {
                callback(nullptr);
            }
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            ++send_calls;
            callback(nullptr);
        }

        void receive(const receive_callback& callback) override {
            ++receive_calls;
            m_receive_callback = callback;
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_start(const std::exception_ptr& error = nullptr) {
            auto callback = m_start_callback;
            if (callback) {
                callback(error);
            }
        }

        // ReSharper disable once CppMemberFunctionMayBeConst
        void emit_stop(const std::exception_ptr& error = nullptr) {
            auto callback = m_stop_callback;
            if (callback) {
                callback(error);
            }
        }

        void emit_receive(const std::string& message, const std::exception_ptr& error = nullptr) {
            auto callback = std::move(m_receive_callback);
            m_receive_callback = nullptr;
            if (callback) {
                callback(std::string(message), error);
            }
        }

    private:
        completion_callback m_start_callback;
        completion_callback m_stop_callback;
        receive_callback m_receive_callback;
    };
}

static std::shared_ptr<signalr::transport> make_round7_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );
}

static std::shared_ptr<signalr::transport> make_round7_transport(
    const std::shared_ptr<controlled_round7_websocket_client>& client
) {
    return make_round7_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        }
    );
}

// CONTRACT PROPOSAL: stop() should cancel a start() already in progress, even if the client factory is blocked.
// This test is deterministic; it does not assume that a particular scheduler or network delay will occur.
TEST(websocket_transport_round7, contract_stop_during_client_factory_prevents_late_connection) {
    auto client = std::make_shared<controlled_round7_websocket_client>();
    std::mutex mutex;
    std::condition_variable condition;
    bool entered_factory = false;
    bool allow_factory_to_return = false;
    std::atomic<int> start_successes{0};
    std::atomic<int> start_failures{0};
    std::atomic<int> stop_completions{0};

    auto transport = make_round7_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            std::unique_lock<std::mutex> lock(mutex);
            entered_factory = true;
            condition.notify_all();
            condition.wait(lock, [&]() { return allow_factory_to_return; });
            return client;
        }
    );

    std::thread start_thread([&]() {
        transport->start("ws://example.test", [&](const std::exception_ptr& error) {
            if (error) {
                ++start_failures;
            } else {
                ++start_successes;
            }
        });
    });

    bool observed_factory;
    {
        std::unique_lock<std::mutex> lock(mutex);
        observed_factory = condition.wait_for(
            lock,
            std::chrono::seconds(3),
            [&]() { return entered_factory; }
        );
    }

    if (observed_factory) {
        transport->stop([&](const std::exception_ptr& error) {
            EXPECT_EQ(nullptr, error);
            ++stop_completions;
        });
    }

    {
        std::lock_guard<std::mutex> lock(mutex);
        allow_factory_to_return = true;
    }
    condition.notify_all();
    start_thread.join();

    ASSERT_TRUE(observed_factory);
    EXPECT_EQ(1, stop_completions.load());
    EXPECT_EQ(0, start_successes.load());
    EXPECT_EQ(1, start_failures.load());
    EXPECT_EQ(0, client->start_calls.load());

    // Cleanup even if the current implementation allowed the late connection.
    transport->stop([](const std::exception_ptr&) {});
}

// CONTRACT PROPOSAL: a backend that invokes the start callback inline and then throws must not cause a
// previously accepted stop request to finish before the actual deferred client stop has completed.
TEST(websocket_transport_round7, contract_throw_after_start_success_does_not_complete_pending_stop_early) {
    auto client = std::make_shared<controlled_round7_websocket_client>();
    client->defer_stop = true;
    client->throw_after_start_success = true;
    auto transport = make_round7_transport(client);

    int start_successes = 0;
    int stop_completions = 0;
    int close_notifications = 0;
    transport->on_close([&](const std::exception_ptr&) { ++close_notifications; });

    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++start_successes;
        transport->stop([&](const std::exception_ptr& stop_error) {
            EXPECT_EQ(nullptr, stop_error);
            ++stop_completions;
        });
    });

    EXPECT_EQ(1, start_successes);
    EXPECT_EQ(1, client->stop_calls.load());
    EXPECT_EQ(0, stop_completions);
    EXPECT_EQ(0, close_notifications);

    client->emit_stop();
    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(1, close_notifications);
}

// Positive regression: duplicate underlying stop completions must not notify the application twice.
TEST(websocket_transport_round7, duplicate_backend_stop_completion_has_no_duplicate_notifications) {
    auto client = std::make_shared<controlled_round7_websocket_client>();
    client->defer_stop = true;
    auto transport = make_round7_transport(client);

    int close_count = 0;
    int stop_count = 0;
    transport->on_close([&](const std::exception_ptr&) { ++close_count; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->stop([&](const std::exception_ptr&) { ++stop_count; });

    EXPECT_EQ(0, close_count);
    EXPECT_EQ(0, stop_count);
    client->emit_stop();
    client->emit_stop();

    EXPECT_EQ(1, close_count);
    EXPECT_EQ(1, stop_count);
}

// Positive regression: on_close is allowed to initiate a new session synchronously.
TEST(websocket_transport_round7, on_close_can_reconnect_before_prior_stop_callback) {
    auto first = std::make_shared<controlled_round7_websocket_client>();
    auto second = std::make_shared<controlled_round7_websocket_client>();
    int factory_calls = 0;
    auto transport = make_round7_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            ++factory_calls;
            return factory_calls == 1 ? first : second;
        }
    );

    int close_calls = 0;
    int restart_calls = 0;
    int stop_calls = 0;
    transport->on_close([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++close_calls;
        transport->start("ws://example.test/reconnected", [&](const std::exception_ptr& start_error) {
            EXPECT_EQ(nullptr, start_error);
            ++restart_calls;
        });
    });

    transport->start("ws://example.test/first", [](const std::exception_ptr&) {});
    transport->stop([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++stop_calls;
    });

    EXPECT_EQ(1, close_calls);
    EXPECT_EQ(1, restart_calls);
    EXPECT_EQ(1, stop_calls);
    EXPECT_EQ(2, factory_calls);
    EXPECT_EQ(1, second->start_calls.load());

    // Avoid initiating another reconnect during test cleanup.
    transport->on_close([](const std::exception_ptr&) {});
    transport->stop([](const std::exception_ptr&) {});
}

// Positive regression: a throwing application receive handler must not halt subsequent dispatch.
TEST(websocket_transport_round7, throwing_on_receive_does_not_block_next_message) {
    auto client = std::make_shared<controlled_round7_websocket_client>();
    auto transport = make_round7_transport(client);

    int received = 0;
    transport->on_receive([&](std::string&&, const std::exception_ptr&) {
        ++received;
        if (received == 1) {
            throw std::runtime_error("application receive callback failure");
        }
    });

    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    client->emit_receive("first");
    client->emit_receive("second");

    EXPECT_EQ(2, received);
    EXPECT_EQ(3, client->receive_calls.load());
    transport->stop([](const std::exception_ptr&) {});
}

// Positive regression: send completion may synchronously invoke stop without deadlocking on session mutex.
TEST(websocket_transport_round7, synchronous_send_callback_can_stop_transport) {
    auto client = std::make_shared<controlled_round7_websocket_client>();
    auto transport = make_round7_transport(client);

    int send_count = 0;
    int stop_count = 0;
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->send("payload", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++send_count;
        transport->stop([&](const std::exception_ptr& stop_error) {
            EXPECT_EQ(nullptr, stop_error);
            ++stop_count;
        });
    });

    EXPECT_EQ(1, send_count);
    EXPECT_EQ(1, stop_count);
    EXPECT_EQ(1, client->stop_calls.load());
}

// LIFETIME CONTRACT: a stop callback accepted during a pending start should not be silently discarded merely
// because the transport is subsequently destroyed. The backend client is retained independently by the test.
TEST(websocket_transport_round7, pending_connect_stop_callback_survives_transport_destruction) {
    auto client = std::make_shared<controlled_round7_websocket_client>();
    client->defer_start = true;
    int start_completions = 0;
    int stop_completions = 0;

    {
        auto transport = make_round7_transport(client);
        transport->start("ws://example.test", [&](const std::exception_ptr& error) {
            EXPECT_NE(nullptr, error);
            ++start_completions;
        });
        transport->stop([&](const std::exception_ptr&) { ++stop_completions; });
        EXPECT_EQ(0, stop_completions);
    }

    // The backend is valid and can still deliver the callback it retained.
    client->emit_start();
    EXPECT_EQ(1, start_completions);
    EXPECT_EQ(1, stop_completions);
}

} // namespace websocket_transport_round7

namespace websocket_transport_round8
{
using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class controlled_round8_client final : public signalr::websocket_client {
    public:
        bool defer_start{false};
        bool defer_stop{false};
        bool fail_start{false};
        bool throw_initial_receive{false};
        bool throw_second_receive{false};
        bool inline_first_receive{false};
        bool defer_send{false};

        int start_calls{0};
        int stop_calls{0};
        int receive_calls{0};
        int send_calls{0};

        void start(const std::string&, const completion_callback& callback) override {
            ++start_calls;
            m_start_callback = callback;
            if (!defer_start) {
                if (fail_start) {
                    callback(std::make_exception_ptr(std::runtime_error("connect failed")));
                } else {
                    callback(nullptr);
                }
            }
        }

        void stop(const completion_callback& callback) override {
            ++stop_calls;
            m_stop_callback = callback;
            if (!defer_stop) {
                callback(nullptr);
            }
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            ++send_calls;
            m_send_callback = callback;
            if (!defer_send) {
                callback(nullptr);
            }
        }

        void receive(const receive_callback& callback) override {
            ++receive_calls;
            if (throw_initial_receive && receive_calls == 1) {
                throw std::runtime_error("initial receive failed");
            }
            if (throw_second_receive && receive_calls == 2) {
                throw std::runtime_error("second receive failed");
            }
            m_receive_callback = callback;
            if (inline_first_receive && receive_calls == 1) {
                callback(std::string("first"), nullptr);
            }
        }

        void finish_stop(const std::exception_ptr& error = nullptr) const {
            auto callback = m_stop_callback;
            if (callback) {
                callback(error);
            }
        }

        void finish_receive(const std::string& message, const std::exception_ptr& error = nullptr) {
            auto callback = std::move(m_receive_callback);
            m_receive_callback = nullptr;
            if (callback) {
                callback(std::string(message), error);
            }
        }

        void finish_send(const std::exception_ptr& error = nullptr) const {
            auto callback = m_send_callback;
            if (callback) {
                callback(error);
            }
        }

        void clear_callbacks() {
            m_start_callback = nullptr;
            m_stop_callback = nullptr;
            m_receive_callback = nullptr;
            m_send_callback = nullptr;
        }

    private:
        completion_callback m_start_callback;
        completion_callback m_stop_callback;
        receive_callback m_receive_callback;
        std::function<void(std::exception_ptr)> m_send_callback;
    };
}

static std::shared_ptr<signalr::transport> make_round8_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );
}

static std::shared_ptr<signalr::transport> make_round8_transport(
    const std::shared_ptr<controlled_round8_client>& client
) {
    return make_round8_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        }
    );
}

namespace {
    struct lifetime_observation {
        bool destroyed{false};
        bool destroyed_inside_start{false};
    };
}

namespace {
    class lifetime_probe_client final : public signalr::websocket_client {
    public:
        explicit lifetime_probe_client(std::shared_ptr<lifetime_observation> observation)
            : m_observation(std::move(observation)) {}

        ~lifetime_probe_client() override {
            m_observation->destroyed = true;
        }

        void start(const std::string&, const completion_callback& callback) override {
            // Capture an independently owned observation before invoking reentrant user code.
            auto observation = m_observation;
            callback(nullptr);
            observation->destroyed_inside_start = observation->destroyed;
        }

        void stop(const completion_callback& callback) override {
            callback(nullptr);
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            callback(nullptr);
        }

        void receive(const receive_callback&) override {}

    private:
        std::shared_ptr<lifetime_observation> m_observation;
    };
}

// DEFECT: m_client retains a client which retains a strong session via its start callback.
// A failed start must not leak either object, even when a compliant backend caches the callback.
TEST(websocket_transport_round8, failed_start_releases_client_with_cached_start_callback) {
    auto client = std::make_shared<controlled_round8_client>();
    client->fail_start = true;
    std::weak_ptr<controlled_round8_client> weak_client = client;
    auto transport = make_round8_transport(client);

    int failures = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++failures;
    });

    EXPECT_EQ(1, failures);
    transport.reset();
    client.reset();

    const bool was_released = weak_client.expired();
    if (auto retained = weak_client.lock()) {
        // Clean up the expected-failing case so it does not leak into later tests.
        retained->clear_callbacks();
    }
    EXPECT_TRUE(was_released);
    EXPECT_TRUE(weak_client.expired());
}

// CONTRACT VIOLATION: a start failure must not also raise on_close.
TEST(websocket_transport_round8, first_receive_registration_failure_reports_start_only) {
    auto client = std::make_shared<controlled_round8_client>();
    client->throw_initial_receive = true;
    auto transport = make_round8_transport(client);

    int start_failures = 0;
    int close_notifications = 0;
    transport->on_close([&](const std::exception_ptr&) { ++close_notifications; });
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++start_failures;
    });

    EXPECT_EQ(1, start_failures);
    EXPECT_EQ(0, close_notifications);
    EXPECT_EQ(1, client->stop_calls);
}

// CONTRACT CLARIFICATION: after delivery of the first message, a later receive-registration failure
// should be reported as an established-connection error, not retroactively change the start result.
TEST(websocket_transport_round8, inline_receive_then_second_registration_failure_is_connection_failure) {
    auto client = std::make_shared<controlled_round8_client>();
    client->inline_first_receive = true;
    client->throw_second_receive = true;
    auto transport = make_round8_transport(client);

    int received = 0;
    int start_successes = 0;
    int close_notifications = 0;
    transport->on_receive([&](std::string&& message, const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        EXPECT_EQ("first", message);
        ++received;
    });
    transport->on_close([&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++close_notifications;
    });
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        if (!error) {
            ++start_successes;
        }
    });

    EXPECT_EQ(1, received);
    EXPECT_EQ(2, client->receive_calls);
    EXPECT_EQ(1, start_successes);
    EXPECT_EQ(1, close_notifications);
}

// DEFECT: calling stop() inline from start's success callback can release the last WebSocket-client
// owner while websocket_client::start() is still on its stack. The backend is allowed inline callbacks.
TEST(websocket_transport_round8, client_lives_until_synchronous_start_call_returns) {
    auto observation = std::make_shared<lifetime_observation>();
    auto transport = make_round8_transport(
        [observation](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return std::make_shared<lifetime_probe_client>(observation);
        }
    );

    int starts = 0;
    int stops = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++starts;
        transport->stop([&](const std::exception_ptr& stop_error) {
            EXPECT_EQ(nullptr, stop_error);
            ++stops;
        });
    });

    EXPECT_EQ(1, starts);
    EXPECT_EQ(1, stops);
    EXPECT_FALSE(observation->destroyed_inside_start);
    EXPECT_TRUE(observation->destroyed);
}

// Positive regression: a stop during a blocked factory cancels the reserved session before
// the actual WebSocket client is started. Coordination is via a condition variable, not sleeps.
TEST(websocket_transport_round8, stop_during_factory_cancels_before_client_start) {
    auto client = std::make_shared<controlled_round8_client>();
    std::mutex mutex;
    std::condition_variable condition;
    bool inside_factory = false;
    bool allow_factory_return = false;
    std::atomic<int> start_cancellations{0};
    std::atomic<int> stop_completions{0};

    auto transport = make_round8_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            std::unique_lock<std::mutex> lock(mutex);
            inside_factory = true;
            condition.notify_all();
            condition.wait(lock, [&]() { return allow_factory_return; });
            return client;
        }
    );

    std::thread worker([&]() {
        transport->start("ws://example.test", [&](const std::exception_ptr& error) {
            if (error) {
                ++start_cancellations;
            }
        });
    });

    bool observed_factory;
    {
        std::unique_lock<std::mutex> lock(mutex);
        observed_factory = condition.wait_for(
            lock,
            std::chrono::seconds(3),
            [&]() { return inside_factory; }
        );
    }

    if (observed_factory) {
        transport->stop([&](const std::exception_ptr& error) {
            EXPECT_EQ(nullptr, error);
            ++stop_completions;
        });
    }

    {
        std::lock_guard<std::mutex> lock(mutex);
        allow_factory_return = true;
    }
    condition.notify_all();
    worker.join();

    ASSERT_TRUE(observed_factory);
    EXPECT_EQ(0, client->start_calls);
    EXPECT_EQ(1, start_cancellations.load());
    EXPECT_EQ(1, stop_completions.load());
}

// Positive regression: shutdown callbacks are coalesced; they do not fire before backend completion.
TEST(websocket_transport_round8, overlapping_stop_requests_complete_together_once) {
    auto client = std::make_shared<controlled_round8_client>();
    client->defer_stop = true;
    auto transport = make_round8_transport(client);

    int closes = 0;
    int stops_a = 0;
    int stops_b = 0;
    transport->on_close([&](const std::exception_ptr&) { ++closes; });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    transport->stop([&](const std::exception_ptr&) { ++stops_a; });
    transport->stop([&](const std::exception_ptr&) { ++stops_b; });
    EXPECT_EQ(1, client->stop_calls);
    EXPECT_EQ(0, stops_a);
    EXPECT_EQ(0, stops_b);

    client->finish_stop();
    EXPECT_EQ(1, stops_a);
    EXPECT_EQ(1, stops_b);
    EXPECT_EQ(1, closes);
}

// Positive regression: on_close can synchronously start a replacement session.
TEST(websocket_transport_round8, on_close_can_start_new_session) {
    auto first = std::make_shared<controlled_round8_client>();
    auto second = std::make_shared<controlled_round8_client>();
    int factory_calls = 0;
    auto transport = make_round8_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return ++factory_calls == 1 ? first : second;
        }
    );

    int restart_successes = 0;
    transport->on_close([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        transport->start("ws://example.test/retry", [&](const std::exception_ptr& start_error) {
            EXPECT_EQ(nullptr, start_error);
            ++restart_successes;
        });
    });

    transport->start("ws://example.test/first", [](const std::exception_ptr&) {});
    transport->stop([](const std::exception_ptr&) {});
    EXPECT_EQ(2, factory_calls);
    EXPECT_EQ(1, restart_successes);
    EXPECT_EQ(1, second->start_calls);
    transport->on_close([](const std::exception_ptr&) {});
    transport->stop([](const std::exception_ptr&) {});
}

// CONTRACT PROPOSAL: the transport contract says in-flight sends stop at shutdown, while the
// websocket_client contract only requires stop() to drain receives. Decide whether send callbacks
// must settle before stop reports completion; enable this expectation if that stronger contract is chosen.
TEST(websocket_transport_round8, contract_pending_send_completion_during_stop) {
    auto client = std::make_shared<controlled_round8_client>();
    client->defer_send = true;
    auto transport = make_round8_transport(client);
    int send_completions = 0;
    int stop_completions = 0;
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->send("payload", signalr::transfer_format::text, [&](const std::exception_ptr&) {
        ++send_completions;
    });
    transport->stop([&](const std::exception_ptr&) { ++stop_completions; });

    EXPECT_EQ(1, stop_completions);
    // Under a stronger quiescence contract, assert EXPECT_EQ(1, send_completions) here instead.
    EXPECT_EQ(0, send_completions);
    client->finish_send();
    EXPECT_EQ(1, send_completions);
}

// Positive regression: after successful connection, a receive error produces a close notification,
// rather than a second start completion.
TEST(websocket_transport_round8, receive_error_after_success_notifies_close_once) {
    auto client = std::make_shared<controlled_round8_client>();
    auto transport = make_round8_transport(client);
    int successes = 0;
    int closes = 0;
    transport->on_close([&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++closes;
    });
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++successes;
    });

    client->finish_receive("", std::make_exception_ptr(std::runtime_error("receive failed")));
    EXPECT_EQ(1, successes);
    EXPECT_EQ(1, closes);
    EXPECT_EQ(1, client->stop_calls);
}

// Positive regression: moving the client out of a normally stopping session should prevent
// the client/cached-stop-callback/session reference cycle present in some older revisions.
TEST(websocket_transport_round8, normal_stop_releases_client_with_cached_callbacks) {
    auto client = std::make_shared<controlled_round8_client>();
    std::weak_ptr<controlled_round8_client> weak_client = client;
    auto transport = make_round8_transport(client);
    int stops = 0;

    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->stop([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++stops;
    });
    EXPECT_EQ(1, stops);

    transport.reset();
    client.reset();
    const bool was_released = weak_client.expired();
    if (auto retained = weak_client.lock()) {
        retained->clear_callbacks();
    }
    EXPECT_TRUE(was_released);
}

} // namespace websocket_transport_round8

namespace websocket_transport_round9
{
using completion_callback = std::function<void(const std::exception_ptr&)>;
using receive_callback = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class round9_client final : public signalr::websocket_client {
    public:
        bool defer_start{false};
        bool fail_start{false};
        bool defer_stop{false};
        bool defer_send{false};
        bool fail_first_receive{false};
        bool inline_first_receive{false};
        bool fail_second_receive{false};

        int starts{0};
        int stops{0};
        int receives{0};
        int sends{0};

        void start(const std::string&, const completion_callback& callback) override {
            ++starts;
            m_start = callback;
            if (!defer_start) {
                callback(fail_start ? std::make_exception_ptr(std::runtime_error("connect failed")) : nullptr);
            }
        }

        void stop(const completion_callback& callback) override {
            ++stops;
            m_stop = callback;
            if (!defer_stop) {
                callback(nullptr);
            }
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            ++sends;
            m_send = callback;
            if (!defer_send) {
                callback(nullptr);
            }
        }

        void receive(const receive_callback& callback) override {
            ++receives;
            if (fail_first_receive && receives == 1) {
                throw std::runtime_error("first receive registration failed");
            }
            if (fail_second_receive && receives == 2) {
                throw std::runtime_error("second receive registration failed");
            }
            m_receive = callback;
            if (inline_first_receive && receives == 1) {
                auto first = std::move(m_receive);
                m_receive = nullptr;
                first(std::string("first"), nullptr);
            }
        }

        // ReSharper disable once CppDeclaratorNeverUsed
        void complete_start(const std::exception_ptr& error = nullptr) const {
            auto callback = m_start;
            if (callback) {
                callback(error);
            }
        }

        // ReSharper disable once CppDeclaratorNeverUsed
        void complete_stop(const std::exception_ptr& error = nullptr) const {
            auto callback = m_stop;
            if (callback) {
                callback(error);
            }
        }

        void complete_send(const std::exception_ptr& error = nullptr) const {
            auto callback = m_send;
            if (callback) {
                callback(error);
            }
        }

        void clear_callbacks() {
            m_start = nullptr;
            m_stop = nullptr;
            m_send = nullptr;
            m_receive = nullptr;
        }

    private:
        completion_callback m_start;
        completion_callback m_stop;
        std::function<void(std::exception_ptr)> m_send;
        receive_callback m_receive;
    };
}

namespace {
    class blocked_first_receive_client final : public signalr::websocket_client {
    public:
        void start(const std::string&, const completion_callback& callback) override {
            callback(nullptr);
        }

        void stop(const completion_callback& callback) override {
            ++stop_calls;
            m_stop = callback;
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            callback(nullptr);
        }

        void receive(const receive_callback&) override {
            std::unique_lock<std::mutex> lock(m_mutex);
            entered_receive = true;
            m_cv.notify_all();
            m_cv.wait(lock, [this]() { return allow_failure; });
            throw std::runtime_error("receive registration failed while stop was pending");
        }

        bool wait_for_receive() {
            std::unique_lock<std::mutex> lock(m_mutex);
            return m_cv.wait_for(lock, std::chrono::seconds(3), [this]() { return entered_receive; });
        }

        void release_receive() {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                allow_failure = true;
            }
            m_cv.notify_all();
        }

        void complete_stop() const {
            const auto callback = m_stop;
            if (callback) {
                callback(nullptr);
            }
        }

        std::atomic<int> stop_calls{0};

    private:
        std::mutex m_mutex;
        std::condition_variable m_cv;
        bool entered_receive{false};
        bool allow_failure{false};
        completion_callback m_stop;
    };
}

namespace {
    class throwing_log_writer final : public signalr::log_writer {
    public:
        bool throw_on_connect{false};
        bool throw_on_stop{false};

        void write(const std::string& entry) override {
            if (throw_on_connect && entry.find("connecting to:") != std::string::npos) {
                throw std::runtime_error("log writer rejected connection log");
            }
            if (throw_on_stop && entry.find("stopping websocket transport") != std::string::npos) {
                throw std::runtime_error("log writer rejected stop log");
            }
        }
    };
}

static std::shared_ptr<signalr::transport> make_round9_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory,
    const std::shared_ptr<signalr::log_writer>& writer = std::make_shared<signalr::trace_log_writer>()
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(writer, signalr::trace_level::verbose)
    );
}

static std::shared_ptr<signalr::transport> make_round9_transport(const std::shared_ptr<round9_client>& client) {
    return make_round9_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        }
    );
}

// Previous-round regression: retained start callbacks must not form a failed-start ownership cycle.
TEST(websocket_transport_round9, failed_start_releases_client_even_if_start_callback_is_cached) {
    auto client = std::make_shared<round9_client>();
    client->fail_start = true;
    std::weak_ptr<round9_client> weak_client = client;
    auto transport = make_round9_transport(client);

    int start_failures = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++start_failures;
    });
    EXPECT_EQ(1, start_failures);

    transport.reset();
    client.reset();
    const bool released = weak_client.expired();
    if (auto retained = weak_client.lock()) {
        retained->clear_callbacks();
    }
    EXPECT_TRUE(released);
}

// Previous-round regression: an initial receive-registration failure is a failed start, not on_close.
TEST(websocket_transport_round9, first_receive_registration_failure_does_not_raise_on_close) {
    auto client = std::make_shared<round9_client>();
    client->fail_first_receive = true;
    auto transport = make_round9_transport(client);
    int start_failures = 0;
    int closes = 0;
    transport->on_close([&](const std::exception_ptr&) { ++closes; });
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++start_failures;
    });

    EXPECT_EQ(1, start_failures);
    EXPECT_EQ(0, closes);
    EXPECT_EQ(1, client->stops);
}

// Previous-round regression: an inline first receive followed by a throwing second registration
// must be treated as a post-connection failure rather than a failed initial registration.
TEST(websocket_transport_round9, inline_second_receive_failure_reports_close_and_start_success) {
    auto client = std::make_shared<round9_client>();
    client->inline_first_receive = true;
    client->fail_second_receive = true;
    auto transport = make_round9_transport(client);
    int receives = 0;
    int closes = 0;
    int successes = 0;

    transport->on_receive([&](std::string&& message, const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        EXPECT_EQ("first", message);
        ++receives;
    });
    transport->on_close([&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++closes;
    });
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        if (!error) {
            ++successes;
        }
    });

    EXPECT_EQ(1, receives);
    EXPECT_EQ(1, closes);
    EXPECT_EQ(1, successes);
    EXPECT_EQ(2, client->receives);
}

// Verifies that a failure during connection initialization after session reservation
// does not permanently strand the transport in a connecting state.
TEST(websocket_transport_round9, throwing_connect_websocket_client_factory_does_not_permanently_reserve_session) {
    auto writer = std::make_shared<throwing_log_writer>();
    writer->throw_on_connect = true;
    auto client = std::make_shared<round9_client>();

    int factory_attempts = 0;
    const auto transport = make_round9_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            ++factory_attempts;
            if (factory_attempts == 1) {
                // Simulate an initialization failure after session reservation
                throw std::runtime_error("simulated client initialization failure");
            }
            return client;
        },
        writer
    );

    int first_errors = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++first_errors;
    });

    writer->throw_on_connect = false;

    int retry_successes = 0;
    transport->start("ws://example.test/retry", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        if (!error) {
            ++retry_successes;
        }
    });

    EXPECT_EQ(1, first_errors);
    EXPECT_EQ(1, retry_successes);
    EXPECT_EQ(2, factory_attempts);

    transport->stop([](const std::exception_ptr&) {});
}

// DEFECT: if stop logging fails, shutdown must still reach the client and drain pending callbacks.
TEST(websocket_transport_round9, throwing_stop_log_does_not_abandon_shutdown) {
    auto writer = std::make_shared<throwing_log_writer>();
    auto client = std::make_shared<round9_client>();
    auto transport = make_round9_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        },
        writer
    );
    transport->start("ws://example.test", [](const std::exception_ptr&) {});

    writer->throw_on_stop = true;
    int first_stop_completions = 0;
    transport->stop([&](const std::exception_ptr&) { ++first_stop_completions; });
    writer->throw_on_stop = false;

    int second_stop_completions = 0;
    transport->stop([&](const std::exception_ptr&) { ++second_stop_completions; });

    EXPECT_EQ(1, client->stops);
    EXPECT_EQ(1, first_stop_completions);
    EXPECT_EQ(1, second_stop_completions);
}

// DEFECT: a receive-registration error racing with an already-started stop must not mark
// shutdown complete before that stop's backend callback has been delivered.
TEST(websocket_transport_round9, initial_receive_failure_during_stop_preserves_stop_completion_barrier) {
    auto client = std::make_shared<blocked_first_receive_client>();
    auto transport = make_round9_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        }
    );
    std::atomic<int> stop_completions{0};
    std::atomic<int> closes{0};
    std::atomic<int> start_completions{0};
    transport->on_close([&](const std::exception_ptr&) { ++closes; });

    std::thread starting([&]() {
        transport->start("ws://example.test", [&](const std::exception_ptr&) { ++start_completions; });
    });
    const bool entered = client->wait_for_receive();
    if (entered) {
        transport->stop([&](const std::exception_ptr&) { ++stop_completions; });
        EXPECT_EQ(1, client->stop_calls.load());
    }
    client->release_receive();
    starting.join();
    ASSERT_TRUE(entered);

    EXPECT_EQ(0, stop_completions.load()) << "Backend has not acknowledged stop yet";
    client->complete_stop();
    EXPECT_EQ(1, stop_completions.load());
    EXPECT_EQ(1, start_completions.load());
    EXPECT_EQ(0, closes.load()) << "A failed initial registration must not produce on_close";
}

// The accepted weak contract: stop() may finish before an already accepted send callback.
// It must remain safe to deliver that send completion later, and stop() must remain nonblocking.
TEST(websocket_transport_round9, pending_send_callback_may_run_after_stop_completion) {
    auto client = std::make_shared<round9_client>();
    client->defer_send = true;
    auto transport = make_round9_transport(client);
    int send_completions = 0;
    int stop_completions = 0;
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->send("payload", signalr::transfer_format::text, [&](const std::exception_ptr&) {
        ++send_completions;
    });
    transport->stop([&](const std::exception_ptr&) { ++stop_completions; });

    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(0, send_completions);
    client->complete_send();
    EXPECT_EQ(1, send_completions);
}

// CONTRACT PROPOSAL: at-most-once does not ensure that a pending start ever completes.
// Enable this only after defining cancellation/eventual completion for a never-completing backend start.
TEST(websocket_transport_round9, DISABLED_pending_start_without_completion_does_not_leak_after_transport_destruction) {
    auto client = std::make_shared<round9_client>();
    client->defer_start = true;
    std::weak_ptr<round9_client> weak_client = client;
    auto transport = make_round9_transport(client);
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->stop([](const std::exception_ptr&) {});
    transport.reset();
    client.reset();

    const bool released = weak_client.expired();
    if (auto retained = weak_client.lock()) {
        retained->clear_callbacks();
    }
    EXPECT_TRUE(released);
}

// CONTRACT PROPOSAL: an on_close event should not precede publication of start success.
// Current interfaces do not explicitly guarantee this ordering. Enable only after deciding it.
TEST(websocket_transport_round9, DISABLED_on_close_is_not_delivered_before_start_completion) {
    auto client = std::make_shared<round9_client>();
    client->inline_first_receive = true;
    client->fail_second_receive = true;
    auto transport = make_round9_transport(client);
    std::vector<std::string> events;
    transport->on_close([&](const std::exception_ptr&) { events.emplace_back("close"); });
    transport->start("ws://example.test", [&](const std::exception_ptr&) { events.emplace_back("start"); });

    ASSERT_EQ(2u, events.size());
    EXPECT_EQ("start", events.front());
    EXPECT_EQ("close", events.back());
}

} // namespace websocket_transport_round9

namespace websocket_transport_round10
{
using round10_completion = std::function<void(const std::exception_ptr&)>;
using round10_receive = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    class round10_client final : public signalr::websocket_client {
    public:
        bool defer_start{false};
        bool defer_stop{false};
        bool defer_send{false};
        bool block_first_receive{false};
        bool throw_after_receive_release{false};

        std::atomic<int> start_calls{0};
        std::atomic<int> stop_calls{0};
        std::atomic<int> receive_calls{0};
        std::atomic<int> send_calls{0};

        void start(const std::string&, const round10_completion& callback) override {
            ++start_calls;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_start_callback = callback;
            }
            if (!defer_start) {
                callback(nullptr);
            }
        }

        void stop(const round10_completion& callback) override {
            ++stop_calls;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_stop_callback = callback;
            }
            if (!defer_stop) {
                callback(nullptr);
            }
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            ++send_calls;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_send_callback = callback;
            }
            if (!defer_send) {
                callback(nullptr);
            }
        }

        void receive(const round10_receive& callback) override {
            const int registration = ++receive_calls;
            if (registration == 1 && block_first_receive) {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_receive_entered = true;
                m_cv.notify_all();
                m_cv.wait_for(lock, std::chrono::seconds(3), [this]() { return m_release_receive; });
                if (throw_after_receive_release) {
                    throw std::runtime_error("first receive registration failed");
                }
            }
            std::lock_guard<std::mutex> lock(m_mutex);
            m_receive_callback = callback;
        }

        bool wait_for_first_receive() {
            std::unique_lock<std::mutex> lock(m_mutex);
            return m_cv.wait_for(lock, std::chrono::seconds(3), [this]() { return m_receive_entered; });
        }

        void release_first_receive() {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_release_receive = true;
            }
            m_cv.notify_all();
        }

        void complete_start(const std::exception_ptr& error) {
            round10_completion callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_start_callback;
            }
            if (callback) {
                callback(error);
            }
        }

        void complete_stop() {
            round10_completion callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_stop_callback;
            }
            if (callback) {
                callback(nullptr);
            }
        }

        void complete_send() {
            std::function<void(std::exception_ptr)> callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = m_send_callback;
            }
            if (callback) {
                callback(nullptr);
            }
        }

        void deliver_message(std::string payload) {
            round10_receive callback;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                callback = std::move(m_receive_callback);
            }
            if (callback) {
                callback(std::move(payload), nullptr);
            }
        }

    private:
        std::mutex m_mutex;
        std::condition_variable m_cv;
        bool m_receive_entered{false};
        bool m_release_receive{false};
        round10_completion m_start_callback;
        round10_completion m_stop_callback;
        std::function<void(std::exception_ptr)> m_send_callback;
        round10_receive m_receive_callback;
    };
}

namespace {
    class round10_throwing_writer final : public signalr::log_writer {
    public:
        void write(const std::string&) override {
            throw std::runtime_error("writer deliberately failed");
        }
    };
}

static std::shared_ptr<signalr::transport> make_round10_transport(
    const std::function<std::shared_ptr<signalr::websocket_client>(const signalr::signalr_client_config&)>& factory,
    const std::shared_ptr<signalr::log_writer>& writer = std::make_shared<signalr::trace_log_writer>()
) {
    return signalr::websocket_transport::create(
        factory,
        signalr::signalr_client_config{},
        signalr::logger(writer, signalr::trace_level::debug)
    );
}

static std::shared_ptr<signalr::transport> make_round10_transport(const std::shared_ptr<round10_client>& client) {
    return make_round10_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        }
    );
}

// Regression: a factory failure after reserving a session must not prevent a later successful start.
TEST(websocket_transport_round10, factory_failure_releases_reservation_for_retry) {
    auto client = std::make_shared<round10_client>();
    int factory_calls = 0;
    auto transport = make_round10_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            if (++factory_calls == 1) {
                throw std::runtime_error("factory failed");
            }
            return client;
        }
    );

    int errors = 0;
    int successes = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++errors;
    });
    transport->start("ws://example.test/retry", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        if (!error) {
            ++successes;
        }
    });

    EXPECT_EQ(2, factory_calls);
    EXPECT_EQ(1, errors);
    EXPECT_EQ(1, successes);
    transport->stop([](const std::exception_ptr&) {});
}

// Integration regression: the actual SignalR logger absorbs exceptions from a custom log writer.
// This test deliberately uses the real logger rather than treating writer exceptions as transport errors.
TEST(websocket_transport_round10, throwing_log_writer_does_not_cancel_valid_start_or_stop) {
    auto client = std::make_shared<round10_client>();
    auto writer = std::make_shared<round10_throwing_writer>();
    auto transport = make_round10_transport(
        [client](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return client;
        },
        writer
    );

    int start_completions = 0;
    int stop_completions = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++start_completions;
    });
    transport->stop([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++stop_completions;
    });

    EXPECT_EQ(1, start_completions);
    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(1, client->start_calls.load());
    EXPECT_EQ(1, client->stop_calls.load());
}

// Regression: a stop issued while the client factory is blocked must cancel the reserved session.
TEST(websocket_transport_round10, stop_during_client_factory_cancels_before_backend_start) {
    auto client = std::make_shared<round10_client>();
    std::mutex mutex;
    std::condition_variable cv;
    bool factory_entered = false;
    bool release_factory = false;
    std::atomic<int> start_errors{0};
    std::atomic<int> stop_completions{0};

    auto transport = make_round10_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            std::unique_lock<std::mutex> lock(mutex);
            factory_entered = true;
            cv.notify_all();
            cv.wait_for(lock, std::chrono::seconds(3), [&]() { return release_factory; });
            return client;
        }
    );

    std::thread worker([&]() {
        transport->start("ws://example.test", [&](const std::exception_ptr& error) {
            if (error) {
                ++start_errors;
            }
        });
    });

    bool entered;
    {
        std::unique_lock<std::mutex> lock(mutex);
        entered = cv.wait_for(lock, std::chrono::seconds(3), [&]() { return factory_entered; });
    }
    if (entered) {
        transport->stop([&](const std::exception_ptr&) { ++stop_completions; });
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        release_factory = true;
    }
    cv.notify_all();
    worker.join();

    ASSERT_TRUE(entered);
    EXPECT_EQ(1, start_errors.load());
    EXPECT_EQ(1, stop_completions.load());
    EXPECT_EQ(0, client->start_calls.load());
}

// Regression: a deferred start failure must resolve a stop already requested during connecting.
TEST(websocket_transport_round10, failed_connect_drains_pending_stop_once) {
    auto client = std::make_shared<round10_client>();
    client->defer_start = true;
    auto transport = make_round10_transport(client);
    int start_errors = 0;
    int stop_completions = 0;
    int closes = 0;
    transport->on_close([&](const std::exception_ptr&) { ++closes; });

    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_NE(nullptr, error);
        ++start_errors;
    });
    transport->stop([&](const std::exception_ptr&) { ++stop_completions; });
    EXPECT_EQ(0, stop_completions);
    client->complete_start(std::make_exception_ptr(std::runtime_error("connection failed")));

    EXPECT_EQ(1, start_errors);
    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(0, closes);
    EXPECT_EQ(0, client->stop_calls.load());
}

// Regression: deferred stop requests are coalesced, and on_close can reconnect synchronously.
TEST(websocket_transport_round10, deferred_stop_coalesces_and_on_close_can_reconnect) {
    auto first = std::make_shared<round10_client>();
    first->defer_stop = true;
    auto second = std::make_shared<round10_client>();
    int factory_calls = 0;
    auto transport = make_round10_transport(
        [&](const signalr::signalr_client_config&) -> std::shared_ptr<signalr::websocket_client> {
            return ++factory_calls == 1 ? first : second;
        }
    );
    std::weak_ptr<signalr::transport> weak_transport = transport;
    int closes = 0;
    int reconnects = 0;
    int first_stop_completions = 0;
    int second_stop_completions = 0;

    transport->on_close([&](const std::exception_ptr&) {
        if (++closes == 1) {
            auto current = weak_transport.lock();
            ASSERT_NE(nullptr, current);
            current->start("ws://example.test/reconnected", [&](const std::exception_ptr& error) {
                EXPECT_EQ(nullptr, error);
                ++reconnects;
            });
        }
    });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->stop([&](const std::exception_ptr&) { ++first_stop_completions; });
    transport->stop([&](const std::exception_ptr&) { ++second_stop_completions; });

    EXPECT_EQ(0, closes);
    EXPECT_EQ(0, first_stop_completions);
    EXPECT_EQ(0, second_stop_completions);
    EXPECT_EQ(1, first->stop_calls.load());
    first->complete_stop();

    EXPECT_EQ(1, closes);
    EXPECT_EQ(1, reconnects);
    EXPECT_EQ(1, first_stop_completions);
    EXPECT_EQ(1, second_stop_completions);
    EXPECT_EQ(2, factory_calls);
    transport->stop([](const std::exception_ptr&) {});
}

// Regression: an already accepted send may complete after stop and after the transport is destroyed.
TEST(websocket_transport_round10, pending_send_callback_can_complete_after_transport_destruction) {
    auto client = std::make_shared<round10_client>();
    client->defer_send = true;
    auto transport = make_round10_transport(client);
    int send_completions = 0;
    int stop_completions = 0;
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    transport->send("payload", signalr::transfer_format::text, [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++send_completions;
    });
    transport->stop([&](const std::exception_ptr&) { ++stop_completions; });
    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(0, send_completions);
    transport.reset();

    client->complete_send();
    EXPECT_EQ(1, send_completions);
}

// Regression: invoking stop from an on_receive callback must not deadlock the receive trampoline.
TEST(websocket_transport_round10, on_receive_can_synchronously_stop_transport) {
    auto client = std::make_shared<round10_client>();
    auto transport = make_round10_transport(client);
    int receives = 0;
    int stop_completions = 0;
    int closes = 0;
    transport->on_close([&](const std::exception_ptr&) { ++closes; });
    std::weak_ptr<signalr::transport> weak_transport = transport;
    transport->on_receive([&](std::string&& payload, const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        EXPECT_EQ("data", payload);
        ++receives;
        auto current = weak_transport.lock();
        if (current) {
            current->stop([&](const std::exception_ptr&) { ++stop_completions; });
        }
    });
    transport->start("ws://example.test", [](const std::exception_ptr&) {});
    client->deliver_message("data");

    EXPECT_EQ(1, receives);
    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(1, closes);
    EXPECT_EQ(1, client->receive_calls.load());
}

// REGRESSION EXPOSING A DEFECT: stop can finish while the first receive registration is blocked;
// the later registration failure then reports a failed start, even though on_close was already emitted.
// This contradicts transport.h's rule that failed starts do not produce on_close.
TEST(websocket_transport_round10, stop_completed_before_first_receive_failure_does_not_emit_on_close) {
    auto client = std::make_shared<round10_client>();
    client->block_first_receive = true;
    client->throw_after_receive_release = true;
    auto transport = make_round10_transport(client);
    std::atomic<int> start_errors{0};
    std::atomic<int> close_notifications{0};
    std::atomic<int> stop_completions{0};
    transport->on_close([&](const std::exception_ptr&) { ++close_notifications; });

    std::thread starting([&]() {
        transport->start("ws://example.test", [&](const std::exception_ptr& error) {
            if (error) {
                ++start_errors;
            }
        });
    });

    const bool entered = client->wait_for_first_receive();
    if (entered) {
        transport->stop([&](const std::exception_ptr&) { ++stop_completions; });
    }
    client->release_first_receive();
    starting.join();

    ASSERT_TRUE(entered);
    EXPECT_EQ(1, client->stop_calls.load());
    EXPECT_EQ(1, stop_completions.load());
    EXPECT_EQ(1, start_errors.load());
    EXPECT_EQ(0, close_notifications.load()) << "A failed start must not produce on_close";
}

// CONTRACT PROPOSAL ONLY: once stop completed before first registration returns, a start-success
// callback should not subsequently be published. This stronger ordering is not in transport.h.
TEST(websocket_transport_round10, DISABLED_stop_before_first_registration_returns_prevents_late_start_success) {
    auto client = std::make_shared<round10_client>();
    client->block_first_receive = true;
    auto transport = make_round10_transport(client);
    std::atomic<int> start_successes{0};
    std::thread starting([&]() {
        transport->start("ws://example.test", [&](const std::exception_ptr& error) {
            if (!error) {
                ++start_successes;
            }
        });
    });

    const bool entered = client->wait_for_first_receive();
    if (entered) {
        transport->stop([](const std::exception_ptr&) {});
    }
    client->release_first_receive();
    starting.join();

    ASSERT_TRUE(entered);
    EXPECT_EQ(0, start_successes.load());
}

} // namespace websocket_transport_round10

namespace websocket_transport_lifetime {

using stop_completion = std::function<void(const std::exception_ptr&)>;
using receive_completion = std::function<void(std::string&&, const std::exception_ptr&)>;

namespace {
    struct deferred_stop_observation {
        int client_destructions{0};
        int stop_calls{0};
        stop_completion pending_stop_completion;
    };

    class deferred_stop_lifetime_client final : public signalr::websocket_client {
    public:
        explicit deferred_stop_lifetime_client(std::shared_ptr<deferred_stop_observation> observation)
            : m_observation(std::move(observation)) {}

        ~deferred_stop_lifetime_client() override {
            ++m_observation->client_destructions;
        }

        void start(const std::string&, const stop_completion& callback) override {
            callback(nullptr);
        }

        void stop(const stop_completion& callback) override {
            ++m_observation->stop_calls;

            // Quiesce the outstanding read, as the websocket_client contract requires.
            auto pending_receive = std::move(m_pending_receive);
            if (pending_receive) {
                pending_receive(std::string(), nullptr);
            }

            // The close is still pending. The TEST owns this state; it does not own the client.
            m_observation->pending_stop_completion = callback;
        }

        void send(
            const std::string&,
            signalr::transfer_format,
            const std::function<void(std::exception_ptr)>& callback
        ) override {
            callback(nullptr);
        }

        void receive(const receive_completion& callback) override {
            m_pending_receive = callback;
        }

    private:
        std::shared_ptr<deferred_stop_observation> m_observation;
        receive_completion m_pending_receive;
    };
}

TEST(websocket_transport_lifetime, client_remains_alive_until_deferred_stop_completion) {
    auto observation = std::make_shared<deferred_stop_observation>();
    std::weak_ptr<signalr::websocket_client> client_weak;

    // Important: do NOT keep an owning client shared_ptr in the test or factory capture.
    // The transport must retain the client on its own until the stop completion arrives.
    auto transport = signalr::websocket_transport::create(
        [&observation, &client_weak](
            const signalr::signalr_client_config&
        ) -> std::shared_ptr<signalr::websocket_client> {
            auto client = std::make_shared<deferred_stop_lifetime_client>(observation);
            client_weak = client;
            return client;
        },
        signalr::signalr_client_config{},
        signalr::logger(std::make_shared<signalr::trace_log_writer>(), signalr::trace_level::none)
    );

    int start_completions = 0;
    transport->start("ws://example.test", [&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++start_completions;
    });
    ASSERT_EQ(1, start_completions);
    ASSERT_FALSE(client_weak.expired());

    int stop_completions = 0;
    transport->stop([&](const std::exception_ptr& error) {
        EXPECT_EQ(nullptr, error);
        ++stop_completions;
    });

    ASSERT_EQ(1, observation->stop_calls);
    ASSERT_TRUE(static_cast<bool>(observation->pending_stop_completion));
    EXPECT_EQ(0, stop_completions);

    // REGRESSION ASSERTIONS: both fail on websocket_transport(9).cpp.
    // stop() returns with the local client_to_stop destroyed, but its callback is still pending.
    EXPECT_EQ(0, observation->client_destructions);
    EXPECT_FALSE(client_weak.expired());

    // Finish the deferred close without touching a potentially destroyed mock object.
    auto complete_stop = std::move(observation->pending_stop_completion);
    observation->pending_stop_completion = nullptr;
    complete_stop(nullptr);
    complete_stop = nullptr;

    EXPECT_EQ(1, stop_completions);
    EXPECT_EQ(1, observation->client_destructions);
    EXPECT_TRUE(client_weak.expired());
}

} // namespace websocket_transport_lifetime
