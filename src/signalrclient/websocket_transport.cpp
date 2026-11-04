// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "websocket_transport.h"
#include "base_uri.h"
#include "cancellation_token_source.h"
#include "logger.h"
#include "signalrclient/signalr_exception.h"

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 5204 4355)
#endif

#include <atomic>
#include <future>
#include <utility>
#include <vector>

#ifdef _MSC_VER
#pragma warning(pop)
#endif

namespace signalr {

namespace {

using completion_callback = std::function<void(const std::exception_ptr&)>;

completion_callback make_safe_callback(
    const completion_callback& callback,
    const logger& logger
) {
    if (!callback) {
        return [](const std::exception_ptr&) {};
    }
    auto invoked = std::make_shared<std::atomic<bool>>(false);
    return [callback, invoked, logger](const std::exception_ptr& ex) {
        if (!invoked->exchange(true)) {
            try {
                callback(ex);
            } catch (const std::exception& e) {
                logger.logv(
                    trace_level::error,
                    "[websocket transport] user callback threw an exception: ",
                    e.what()
                );
            } catch (...) {
                logger.log(
                    trace_level::error,
                    "[websocket transport] user callback threw an unknown exception"
                );
            }
        }
    };
}

enum class session_state {
    connecting,
    connected,
    disconnecting,
    disconnected
};

} // namespace

class connection_session : public std::enable_shared_from_this<connection_session> {
public:
    connection_session(
        uint64_t id,
        std::shared_ptr<websocket_client> client,
        std::weak_ptr<websocket_transport> transport,
        const logger& logger
    ) : m_id(id),
        m_client(std::move(client)),
        m_transport(std::move(transport)),
        m_logger(logger),
        m_state(session_state::connecting),
        m_has_connected(false),
        m_start_completed(false),
        m_receive_token_source(std::make_shared<cancellation_token_source>()),
        m_is_looping(false),
        m_should_continue(false) {
        m_receive_token_source->cancel();
    }

    uint64_t id() const noexcept {
        return m_id;
    }

    bool is_disconnected() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_state == session_state::disconnected;
    }

    void start(
        const std::string& url,
        const completion_callback& user_callback
    ) noexcept {
        auto safe_cb = make_safe_callback(user_callback, m_logger);
        try {
            bool already_canceled = false;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_state == session_state::disconnecting || m_state == session_state::disconnected) {
                    already_canceled = true;
                } else {
                    m_receive_token_source->reset();
                }
            }

            if (already_canceled) {
                safe_cb(std::make_exception_ptr(signalr::canceled_exception()));
                complete_stop(nullptr);
                return;
            }

            auto weak_self = std::weak_ptr<connection_session>(shared_from_this());

            std::shared_ptr<websocket_client> client;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                client = m_client;
            }

            if (!client) {
                safe_cb(std::make_exception_ptr(signalr_exception("client is null")));
                return;
            }

            client->start(
                url,
                [weak_self, safe_cb](const std::exception_ptr& ex_ptr) {
                    auto session = weak_self.lock();
                    if (!session) {
                        safe_cb(std::make_exception_ptr(signalr_exception("connection session no longer exists")));
                        return;
                    }

                    bool abort_connection = false;
                    bool is_failure = false;
                    std::vector<completion_callback> stop_cbs;

                    {
                        std::lock_guard<std::mutex> lock(session->m_mutex);
                        if (session->m_start_completed) {
                            return; // Drop duplicate start completions
                        }
                        session->m_start_completed = true;

                        if (session->m_state == session_state::connecting) {
                            if (ex_ptr == nullptr) {
                                session->m_state = session_state::connected;
                                session->m_has_connected = true;
                            } else {
                                session->m_state = session_state::disconnected;
                                is_failure = true;
                                stop_cbs.swap(session->m_stop_callbacks);
                            }
                        } else if (session->m_state == session_state::disconnecting) {
                            if (ex_ptr == nullptr) {
                                abort_connection = true;
                            } else {
                                session->m_state = session_state::disconnected;
                                is_failure = true;
                                stop_cbs.swap(session->m_stop_callbacks);
                            }
                        } else {
                            return;
                        }
                    }

                    if (is_failure) {
                        try {
                            std::rethrow_exception(ex_ptr);
                        } catch (const std::exception& e) {
                            session->m_logger.log(
                                trace_level::error,
                                std::string("[websocket transport] exception when connecting to the server: ")
                                    .append(e.what())
                            );
                        } catch (...) {
                            session->m_logger.log(
                                trace_level::error,
                                "[websocket transport] unknown exception when connecting to the server"
                            );
                        }

                        auto transport = session->m_transport.lock();
                        if (transport) {
                            transport->clear_active_session(session->m_id);
                        }

                        safe_cb(ex_ptr);

                        for (const auto& cb : stop_cbs) {
                            try {
                                cb(nullptr);
                            } catch (...) {}
                        }
                        return;
                    }

                    if (abort_connection) {
                        std::shared_ptr<websocket_client> client_to_stop;
                        {
                            std::lock_guard<std::mutex> lock(session->m_mutex);
                            client_to_stop = std::move(session->m_client);
                        }

                        if (client_to_stop) {
                            try {
                                client_to_stop->stop(
                                    [session](const std::exception_ptr& err) {
                                        session->complete_stop(err);
                                    }
                                );
                            } catch (...) {
                                session->complete_stop(std::current_exception());
                            }
                        } else {
                            session->complete_stop(nullptr);
                        }

                        safe_cb(std::make_exception_ptr(signalr::canceled_exception()));
                        return;
                    }

                    try {
                        session->receive_loop(true);
                        safe_cb(nullptr);
                    } catch (...) {
                        auto receive_ex = std::current_exception();
                        session->m_logger.log(
                            trace_level::error,
                            "[websocket transport] exception while registering first receive loop"
                        );

                        std::shared_ptr<websocket_client> client_to_stop;
                        {
                            std::lock_guard<std::mutex> lock(session->m_mutex);
                            session->m_state = session_state::disconnecting;
                            client_to_stop = std::move(session->m_client);
                        }

                        if (client_to_stop) {
                            try {
                                client_to_stop->stop(
                                    [session](const std::exception_ptr&) {
                                        session->complete_stop(nullptr);
                                    }
                                );
                            } catch (...) {
                                session->complete_stop(nullptr);
                            }
                        } else {
                            session->complete_stop(nullptr);
                        }

                        safe_cb(receive_ex);
                    }
                }
            );
        } catch (const std::exception& e) {
            auto ex = std::current_exception();
            bool already_started = false;
            std::vector<completion_callback> stop_cbs;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_state == session_state::connected) {
                    already_started = true;
                } else {
                    m_state = session_state::disconnected;
                    stop_cbs.swap(m_stop_callbacks);
                }
            }

            if (!already_started) {
                m_logger.log(
                    trace_level::error,
                    std::string("[websocket transport] exception when connecting to the server: ")
                        .append(e.what())
                );
                auto transport = m_transport.lock();
                if (transport) {
                    transport->clear_active_session(m_id);
                }
                safe_cb(ex);
                for (const auto& cb : stop_cbs) {
                    try {
                        cb(nullptr);
                    } catch (...) {}
                }
            }
        } catch (...) {
            auto ex = std::current_exception();
            bool already_started = false;
            std::vector<completion_callback> stop_cbs;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_state == session_state::connected) {
                    already_started = true;
                } else {
                    m_state = session_state::disconnected;
                    stop_cbs.swap(m_stop_callbacks);
                }
            }

            if (!already_started) {
                m_logger.log(
                    trace_level::error,
                    "[websocket transport] unknown exception when connecting to the server"
                );
                auto transport = m_transport.lock();
                if (transport) {
                    transport->clear_active_session(m_id);
                }
                safe_cb(ex);
                for (const auto& cb : stop_cbs) {
                    try {
                        cb(nullptr);
                    } catch (...) {}
                }
            }
        }
    }

    void stop(const completion_callback& user_callback) noexcept {
        auto safe_cb = make_safe_callback(user_callback, m_logger);
        try {
            bool initiate_client_stop = false;
            bool already_disconnected = false;
            std::shared_ptr<websocket_client> client_to_stop;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_state == session_state::disconnected) {
                    already_disconnected = true;
                } else {
                    m_stop_callbacks.emplace_back(safe_cb);
                    if (m_state == session_state::connected) {
                        m_state = session_state::disconnecting;
                        initiate_client_stop = true;
                        client_to_stop = std::move(m_client);
                    } else if (m_state == session_state::connecting || m_state == session_state::disconnecting) {
                        m_state = session_state::disconnecting;
                        return;
                    }
                }
            }

            if (already_disconnected) {
                safe_cb(nullptr);
                return;
            }

            if (initiate_client_stop) {
                m_logger.log(
                    trace_level::debug,
                    "stopping websocket transport"
                );

                if (client_to_stop) {
                    // MUST BE CAPTURED BY VALUE TO PREVENT DANGLING STACK REFERENCES
                    auto self = shared_from_this();
                    logger logger = m_logger;

                    try {
                        client_to_stop->stop(
                            [self, logger](const std::exception_ptr& ex_ptr) {
                                if (ex_ptr != nullptr) {
                                    try {
                                        std::rethrow_exception(ex_ptr);
                                    } catch (const std::exception& e) {
                                        logger.logv(
                                            trace_level::error,
                                            "websocket transport stopped with error: ",
                                            e.what()
                                        );
                                    } catch (...) {
                                        logger.log(
                                            trace_level::error,
                                            "websocket transport stopped with unknown error"
                                        );
                                    }
                                } else {
                                    logger.log(
                                        trace_level::debug,
                                        "websocket transport stopped"
                                    );
                                }

                                self->complete_stop(ex_ptr);
                            }
                        );
                    } catch (...) {
                        complete_stop(std::current_exception());
                    }
                } else {
                    complete_stop(nullptr);
                }
            }
        } catch (...) {
            safe_cb(std::current_exception());
        }
    }

    void send(
        const std::string& payload,
        const transfer_format format,
        const completion_callback& user_callback
    ) const noexcept {
        auto safe_cb = make_safe_callback(user_callback, m_logger);
        try {
            std::shared_ptr<websocket_client> client;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_state == session_state::connected) {
                    client = m_client;
                }
            }

            if (!client) {
                safe_cb(std::make_exception_ptr(
                    signalr_exception("cannot send data when transport is not connected")
                ));
                return;
            }

            // Called strictly outside m_mutex to prevent deadlocks when callbacks invoke stop() synchronously
            client->send(
                payload,
                format,
                [safe_cb](const std::exception_ptr& exception) {
                    safe_cb(exception);
                }
            );
        } catch (...) {
            safe_cb(std::current_exception());
        }
    }

    void receive_loop(bool is_initial = false) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_is_looping) {
                m_should_continue = true;
                return;
            }
            m_is_looping = true;
        }

        struct loop_guard {
            std::mutex& mtx;
            bool& looping;
            ~loop_guard() {
                std::lock_guard<std::mutex> lock(mtx);
                looping = false;
            }
        } guard{m_mutex, m_is_looping};

        auto weak_self = std::weak_ptr<connection_session>(shared_from_this());
        auto token_source = m_receive_token_source;

        while (true) {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_should_continue = false;
                if (m_state != session_state::connected) {
                    break;
                }
            }

            auto one_shot_guard = std::make_shared<std::atomic<bool>>(false);
            std::shared_ptr<websocket_client> client;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                client = m_client;
            }

            if (!client) {
                break;
            }

            try {
                client->receive(
                    [weak_self, token_source, one_shot_guard](
                        std::string&& message,
                        std::exception_ptr exception
                    ) {
                        if (one_shot_guard->exchange(true)) {
                            return; // Drop duplicate invocations of the same receive registration
                        }

                        auto session = weak_self.lock();
                        if (!session) {
                            token_source->cancel();
                            return;
                        }

                        bool is_connected;
                        {
                            std::lock_guard<std::mutex> lock(session->m_mutex);
                            is_connected = (session->m_state == session_state::connected);
                        }
                        if (!is_connected) {
                            token_source->cancel();
                            return;
                        }

                        if (exception != nullptr) {
                            try {
                                std::rethrow_exception(exception);
                            } catch (const std::exception& e) {
                                session->m_logger.logv(
                                    trace_level::error,
                                    "[websocket transport] error receiving response from websocket: ",
                                    e.what()
                                );
                            } catch (...) {
                                session->m_logger.log(
                                    trace_level::error,
                                    "[websocket transport] unknown error occurred when receiving response from websocket"
                                );
                                exception = std::make_exception_ptr(signalr_exception("unknown error"));
                            }

                            bool was_connected = false;
                            std::shared_ptr<websocket_client> client_to_stop;
                            {
                                std::lock_guard<std::mutex> lock(session->m_mutex);
                                if (session->m_state == session_state::connected) {
                                    was_connected = true;
                                    session->m_state = session_state::disconnecting;
                                    client_to_stop = std::move(session->m_client);
                                }
                            }

                            token_source->cancel();

                            if (!was_connected) {
                                return;
                            }

                            if (client_to_stop) {
                                auto self = session;
                                try {
                                    client_to_stop->stop(
                                        [self, exception](const std::exception_ptr&) {
                                            self->complete_stop(exception);
                                        }
                                    );
                                } catch (const std::exception& e) {
                                    session->m_logger.logv(
                                        trace_level::error,
                                        "[websocket transport] exception while stopping client after receive error: ",
                                        e.what()
                                    );
                                    session->complete_stop(exception);
                                } catch (...) {
                                    session->m_logger.log(
                                        trace_level::error,
                                        "[websocket transport] unknown exception while stopping client after receive error"
                                    );
                                    session->complete_stop(exception);
                                }
                            } else {
                                session->complete_stop(exception);
                            }
                            return;
                        }

                        auto transport = session->m_transport.lock();
                        if (transport) {
                            transport->notify_receive(session->m_id, std::move(message));
                        }

                        bool continue_receiving;
                        {
                            std::lock_guard<std::mutex> lock(session->m_mutex);
                            continue_receiving = (session->m_state == session_state::connected);
                        }

                        if (continue_receiving) {
                            session->receive_loop(false);
                        } else {
                            token_source->cancel();
                        }
                    }
                );
            } catch (...) {
                if (is_initial) {
                    throw; // Initial registration error bubbles to start() handler
                }

                auto reg_ex = std::current_exception();
                m_logger.log(
                    trace_level::error,
                    "[websocket transport] exception occurred while re-registering receive callback"
                );

                bool was_connected = false;
                std::shared_ptr<websocket_client> client_to_stop;
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    if (m_state == session_state::connected) {
                        was_connected = true;
                        m_state = session_state::disconnecting;
                        client_to_stop = std::move(m_client);
                    }
                }

                token_source->cancel();

                if (was_connected) {
                    if (client_to_stop) {
                        auto self = shared_from_this();
                        try {
                            client_to_stop->stop(
                                [self, reg_ex](const std::exception_ptr&) {
                                    self->complete_stop(reg_ex);
                                }
                            );
                        } catch (...) {
                            complete_stop(reg_ex);
                        }
                    } else {
                        complete_stop(reg_ex);
                    }
                }
                break;
            }

            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (!m_should_continue) {
                    break;
                }
            }
        }
    }

    void complete_stop(const std::exception_ptr& exception) {
        std::vector<completion_callback> callbacks;
        bool should_notify = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_state != session_state::disconnected) {
                m_state = session_state::disconnected;
                should_notify = m_has_connected;
            }
            callbacks.swap(m_stop_callbacks);
            m_client = nullptr; // Breaks DAG ownership cycles
        }

        m_receive_token_source->cancel();

        auto transport = m_transport.lock();
        if (transport) {
            if (should_notify) {
                transport->notify_close(m_id, exception);
            } else {
                transport->clear_active_session(m_id);
            }
        }

        for (const auto& cb : callbacks) {
            try {
                cb(exception);
            } catch (...) {}
        }
    }

private:
    const uint64_t m_id;
    std::shared_ptr<websocket_client> m_client;
    const std::weak_ptr<websocket_transport> m_transport;
    const logger m_logger;

    mutable std::mutex m_mutex;
    session_state m_state;
    bool m_has_connected;
    bool m_start_completed;
    std::shared_ptr<cancellation_token_source> m_receive_token_source;
    std::vector<completion_callback> m_stop_callbacks;
    bool m_is_looping;
    bool m_should_continue;
};

std::shared_ptr<signalr::transport> websocket_transport::create(
    const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_client_factory,
    const signalr_client_config& signalr_client_config,
    const logger& logger
) {
    return std::shared_ptr<transport>(
        new websocket_transport(
            websocket_client_factory,
            signalr_client_config,
            logger
        )
    );
}

websocket_transport::websocket_transport(
    const std::function<std::shared_ptr<websocket_client>(const signalr_client_config&)>& websocket_client_factory,
    signalr_client_config signalr_client_config,
    const logger& logger
) : transport(logger),
    m_websocket_client_factory(websocket_client_factory),
    m_signalr_client_config(std::move(signalr_client_config)),
    m_process_response_callback([](std::string&&, const std::exception_ptr&) {}),
    m_close_callback([](const std::exception_ptr&) {}) {}

websocket_transport::~websocket_transport() {
    std::shared_ptr<connection_session> session;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        session = m_active_session;
        m_active_session = nullptr;
    }

    if (session) {
        try {
            auto promise = std::make_shared<std::promise<void>>();
            session->stop(
                [promise](const std::exception_ptr&) {
                    try {
                        promise->set_value();
                    } catch (...) {}
                }
            );
            auto future = promise->get_future();
            if (future.wait_for(std::chrono::seconds(2)) == std::future_status::timeout) {
                m_logger.log(
                    trace_level::warning,
                    "[websocket transport] timeout waiting for transport shutdown in destructor"
                );
            }
        } catch (...) {}
    }
}

transport_type websocket_transport::get_transport_type() const noexcept {
    return transport_type::websockets;
}

void websocket_transport::start(
    const std::string& url,
    const std::function<void(const std::exception_ptr&)>& user_callback
) noexcept {
    auto safe_cb = make_safe_callback(user_callback, m_logger);
    try {
        const signalr::uri uri(url);
        if (uri.scheme() != "ws" && uri.scheme() != "wss") {
            safe_cb(std::make_exception_ptr(signalr_exception("url must have 'ws' or 'wss' scheme")));
            return;
        }

        bool already_connected = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_active_session && !m_active_session->is_disconnected()) {
                already_connected = true;
            }
        }

        if (already_connected) {
            safe_cb(std::make_exception_ptr(signalr_exception("transport already connected")));
            return;
        }

        const auto query_start = url.find('?');
        m_logger.log(
            trace_level::info,
            "[websocket transport] connecting to: " + url.substr(0, query_start)
        );

        std::shared_ptr<websocket_client> websocket_client;
        try {
            websocket_client = m_websocket_client_factory(m_signalr_client_config);
            if (!websocket_client) {
                safe_cb(std::make_exception_ptr(signalr_exception("websocket client factory returned null")));
                return;
            }
        } catch (...) {
            safe_cb(std::current_exception());
            return;
        }

        std::shared_ptr<connection_session> new_session;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_active_session && !m_active_session->is_disconnected()) {
                already_connected = true;
            } else {
                new_session = std::make_shared<connection_session>(
                    ++m_next_session_id,
                    websocket_client,
                    std::weak_ptr<websocket_transport>(shared_from_this()),
                    m_logger
                );
                m_active_session = new_session;
            }
        }

        if (already_connected) {
            safe_cb(std::make_exception_ptr(signalr_exception("transport already connected")));
            return;
        }

        new_session->start(url, safe_cb);
    } catch (...) {
        safe_cb(std::current_exception());
    }
}

void websocket_transport::stop(const std::function<void(const std::exception_ptr&)>& user_callback) noexcept {
    auto safe_cb = make_safe_callback(user_callback, m_logger);
    std::shared_ptr<connection_session> session;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        session = m_active_session;
    }

    if (!session || session->is_disconnected()) {
        safe_cb(nullptr);
        return;
    }

    session->stop(safe_cb);
}

void websocket_transport::on_close(const std::function<void(const std::exception_ptr&)>& callback) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_close_callback = callback;
}

void websocket_transport::on_receive(
    const std::function<void(std::string&&, const std::exception_ptr&)>& callback
) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_process_response_callback = callback;
}

void websocket_transport::send(
    const std::string& payload,
    transfer_format transfer_format,
    const std::function<void(const std::exception_ptr&)>& user_callback
) noexcept {
    auto safe_cb = make_safe_callback(user_callback, m_logger);
    std::shared_ptr<connection_session> session;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        session = m_active_session;
    }

    if (!session) {
        safe_cb(std::make_exception_ptr(
            signalr_exception("cannot send data when transport is not connected")
        ));
        return;
    }

    session->send(payload, transfer_format, safe_cb);
}

void websocket_transport::notify_receive(uint64_t session_id, std::string&& message) const {
    std::function<void(std::string&&, const std::exception_ptr&)> callback;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_active_session || m_active_session->id() != session_id) {
            return;
        }
        callback = m_process_response_callback;
    }

    if (callback) {
        try {
            callback(std::move(message), nullptr);
        } catch (const std::exception& e) {
            m_logger.logv(
                trace_level::error,
                "[websocket transport] exception in on_receive: ",
                e.what()
            );
        } catch (...) {
            m_logger.log(
                trace_level::error,
                "[websocket transport] unknown exception in on_receive"
            );
        }
    }
}

void websocket_transport::notify_close(uint64_t session_id, const std::exception_ptr& exception) {
    completion_callback callback;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_active_session || m_active_session->id() != session_id) {
            return;
        }
        m_active_session = nullptr;
        callback = m_close_callback;
    }

    if (callback) {
        try {
            callback(exception);
        } catch (const std::exception& e) {
            m_logger.logv(
                trace_level::error,
                "[websocket transport] exception in on_close: ",
                e.what()
            );
        } catch (...) {
            m_logger.log(
                trace_level::error,
                "[websocket transport] unknown exception in on_close"
            );
        }
    }
}

void websocket_transport::clear_active_session(uint64_t session_id) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_active_session && m_active_session->id() == session_id) {
        m_active_session = nullptr;
    }
}

} // namespace signalr
