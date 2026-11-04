// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#ifndef CANCELLATION_TOKEN_SOURCE_H
#define CANCELLATION_TOKEN_SOURCE_H

#include <assert.h>
#include <condition_variable>
#include <forward_list>
#include <mutex>
#include <queue>
#include <vector>
#include <string>

#include "signalrclient/cancellation_token.h"

namespace signalr
{
    class canceled_exception : public std::exception
    {
    public:
        char const* what() const noexcept override {
            return "an operation was canceled";
        }
    };

    class aggregate_exception : public std::exception
    {
    public:
        template <typename iterable>
        static aggregate_exception create(typename iterable::iterator begin, typename iterable::const_iterator end) {
            aggregate_exception exception;
            while (begin != end) {
                exception.add_err_msg(*begin);
                ++begin;
            }
            return exception;
        }

        void add_exception(const std::exception& ex)
        {
            if (!m_message.empty())
            {
                m_message += '\n';
            }
            m_message += ex.what();
        }

        void add_err_msg(const std::string& err_msg)
        {
            if (m_message.empty()) {
                m_message = err_msg;
            } else {
                m_message = err_msg + '\n' + m_message;
            }
        }

        char const* what() const noexcept override {
            return m_message.c_str();
        }
    private:
        std::string m_message;
    };

    class cancellation_token_source
    {
    public:
        static constexpr unsigned int timeout_infinite = 0xFFFFFFFF;

        cancellation_token_source() noexcept
            : m_signaled(false)
        {
        }

        cancellation_token_source(const cancellation_token_source&) = delete;
        cancellation_token_source& operator=(const cancellation_token_source&) = delete;

        // returns false if already canceled, otherwise true if this cancels
        bool cancel() {
            bool signal;
            std::vector<std::function<void()>> callbacks; {
                std::lock_guard<std::mutex> lock(m_lock);
                signal = !m_signaled;
                m_signaled = true;
                m_condition.notify_all();
                callbacks = std::move(m_callbacks);
                m_callbacks = std::vector<std::function<void()>>();
            } // unlock

            if (!callbacks.empty()) {
                std::forward_list<std::string> errors;
                for (auto& func : callbacks) {
                    try {
                        func();
                    } catch (const std::exception& ex) {
                        errors.emplace_front(ex.what());
                    }
                }

                if (!errors.empty()) {
                    throw aggregate_exception::create<std::forward_list<std::string>>(errors.begin(), errors.cend());
                }
            }

            return signal;
        }

        static unsigned int get_infinite_timeout() { return timeout_infinite; }

        void reset()
        {
            std::lock_guard<std::mutex> lock(m_lock);
            assert(m_callbacks.empty());
            m_signaled = false;
            m_callbacks.clear();
        }

        bool is_canceled() const
        {
            return m_signaled;
        }

        unsigned int wait(unsigned int timeout)
        {
            std::unique_lock<std::mutex> lock(m_lock);
            if (timeout == cancellation_token_source::timeout_infinite)
            {
                m_condition.wait(lock, [this]() noexcept { return m_signaled; });
                return 0;
            }
            else
            {
                const std::chrono::milliseconds period(timeout);
                const auto status = m_condition.wait_for(lock, period, [this]() noexcept { return m_signaled; });
                assert(status == m_signaled);
                // Return 0 if the wait completed as a result of signaling the event. Otherwise, return timeout_infinite
                return status ? 0 : cancellation_token_source::timeout_infinite;
            }
        }

        unsigned int wait()
        {
            return wait(cancellation_token_source::timeout_infinite);
        }

        void throw_if_cancellation_requested() const
        {
            if (m_signaled)
            {
                throw canceled_exception();
            }
        }

        void register_callback(const std::function<void()>& callback)
        {
            bool run_callback = false;
            {
                std::lock_guard<std::mutex> lock(m_lock);
                if (m_signaled)
                {
                    run_callback = m_signaled;
                }
                else
                {
                    m_callbacks.push_back(callback);
                }
            } // unlock

            if (run_callback)
            {
                callback();
            }
        }

    private:
        std::mutex m_lock;
        std::condition_variable m_condition;
        bool m_signaled;
        std::vector<std::function<void()>> m_callbacks;
    };

    inline cancellation_token get_cancellation_token(std::weak_ptr<cancellation_token_source> s) {
        return {std::move(s)};
    }
}

#endif // CANCELLATION_TOKEN_SOURCE_H
