// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "signalrclient/http_client.h"
#include "signalrclient/signalr_client_config.h"
#include <future>
#include <vector>
#include <memory>
#include <signalrclient/signalr_value.h>
#include <hub_protocol.h>

std::string remove_date_from_log_entry(const std::string &log_entry);
bool has_log_entry(const std::string& log_entry, const std::vector<std::string>& logs);

std::function<std::shared_ptr<signalr::http_client>(const signalr::signalr_client_config&)> create_test_http_client();
std::string create_uri();
std::string create_uri(const std::string& query_string);
std::vector<std::string> filter_vector(const std::vector<std::string>& source, const std::string& string);
std::string dump_vector(const std::vector<std::string>& source);

template <typename T>
class manual_reset_event
{
public:
    void set(T value) { m_promise.set_value(value); }
    void set(const std::exception& exception) { m_promise.set_exception(std::make_exception_ptr(exception)); }
    void set(std::exception_ptr exception) { m_promise.set_exception(exception); }

    T get()
    {
        try
        {
            auto ret = m_promise.get_future().get();
            m_promise = std::promise<T>();
            return ret;
        }
        catch (...)
        {
            m_promise = std::promise<T>();
            std::rethrow_exception(std::current_exception());
        }
    }
private:
    std::promise<T> m_promise;
};

template <>
class manual_reset_event<void>
{
public:
    void set() { m_promise.set_value(); }
    void set(const std::exception& exception) { m_promise.set_exception(std::make_exception_ptr(exception)); }
    void set(const std::exception_ptr& exception)
    {
        if (exception != nullptr) { m_promise.set_exception(exception); }
        else { m_promise.set_value(); }
    }

    void get()
    {
        try
        {
            m_promise.get_future().get();
        }
        catch (...)
        {
            m_promise = std::promise<void>();
            std::rethrow_exception(std::current_exception());
        }
        m_promise = std::promise<void>();
    }
private:
    std::promise<void> m_promise;
};

class custom_exception : public std::exception
{
public:
    custom_exception() : m_message("custom exception") {}
    custom_exception(const char* message) : m_message(message) { }
    char const* what() const noexcept override { return m_message.data(); }
private:
    std::string m_message;
};

/**
 * Capturing message_handler implementation for protocol testing.
 */
class test_message_handler : public signalr::message_handler
{
public:
    std::vector<signalr::invocation_message> invocation_messages;
    std::vector<signalr::stream_item_message> stream_item_messages;
    std::vector<signalr::completion_message> completion_messages;
    std::vector<signalr::stream_invocation_message> stream_invocation_messages;
    std::vector<signalr::cancel_invocation_message> cancel_invocation_messages;
    std::vector<signalr::ping_message> ping_messages;
    std::vector<signalr::close_message> close_messages;
    std::vector<signalr::ack_message> ack_messages;
    std::vector<signalr::sequence_message> sequence_messages;

    size_t total_messages() const
    {
        return invocation_messages.size() + stream_item_messages.size() +
               completion_messages.size() + stream_invocation_messages.size() +
               cancel_invocation_messages.size() + ping_messages.size() +
               close_messages.size() + ack_messages.size() + sequence_messages.size();
    }

    void handle_invocation_message(signalr::invocation_message&& msg) noexcept override
    {
        invocation_messages.push_back(std::move(msg));
    }
    void handle_stream_item_message(signalr::stream_item_message&& msg) noexcept override
    {
        stream_item_messages.push_back(std::move(msg));
    }
    void handle_completion_message(signalr::completion_message&& msg) noexcept override
    {
        completion_messages.push_back(std::move(msg));
    }
    void handle_stream_invocation_message(signalr::stream_invocation_message&& msg) noexcept override
    {
        stream_invocation_messages.push_back(std::move(msg));
    }
    void handle_cancel_invocation_message(signalr::cancel_invocation_message&& msg) noexcept override
    {
        cancel_invocation_messages.push_back(std::move(msg));
    }
    void handle_ping_message(signalr::ping_message&& msg) noexcept override
    {
        ping_messages.push_back(std::move(msg));
    }
    void handle_close_message(signalr::close_message&& msg) noexcept override
    {
        close_messages.push_back(std::move(msg));
    }
    void handle_ack_message(signalr::ack_message&& msg) noexcept override
    {
        ack_messages.push_back(std::move(msg));
    }
    void handle_sequence_message(signalr::sequence_message&& msg) noexcept override
    {
        sequence_messages.push_back(std::move(msg));
    }
};

// Value and message assertion helpers
void assert_signalr_value_equality(const signalr::value& expected, const signalr::value& actual);
void assert_invocation_message_equality(const signalr::invocation_message& expected, const signalr::invocation_message& actual);
void assert_stream_item_message_equality(const signalr::stream_item_message& expected, const signalr::stream_item_message& actual);
void assert_completion_message_equality(const signalr::completion_message& expected, const signalr::completion_message& actual);
void assert_stream_invocation_message_equality(const signalr::stream_invocation_message& expected, const signalr::stream_invocation_message& actual);
void assert_cancel_invocation_message_equality(const signalr::cancel_invocation_message& expected, const signalr::cancel_invocation_message& actual);
void assert_close_message_equality(const signalr::close_message& expected, const signalr::close_message& actual);
void assert_ack_message_equality(const signalr::ack_message& expected, const signalr::ack_message& actual);
void assert_sequence_message_equality(const signalr::sequence_message& expected, const signalr::sequence_message& actual);