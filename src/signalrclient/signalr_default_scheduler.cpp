// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "signalr_default_scheduler.h"

#include <algorithm>
#include <cassert>
#include <thread>
#include <utility>

namespace signalr
{
    signalr_default_scheduler::internals::internals(const size_t thread_count)
        : m_thread_count(thread_count == 0 ? std::max(1u, std::thread::hardware_concurrency()) : thread_count)
    { }

    void signalr_default_scheduler::internals::start() {
        auto self = shared_from_this();

        // Timer thread monitors the priority queue and pushes due tasks to the ready queue
        std::thread([self]() { self->timer_loop(); }).detach();

        // Worker threads execute ready tasks concurrently
        for (size_t i = 0; i < m_thread_count; ++i) {
            std::thread([self]() { self->worker_loop(); }).detach();
        }
    }

    void signalr_default_scheduler::internals::schedule(const signalr_base_cb& cb, std::chrono::milliseconds delay)
    {
        if (!cb) {
            return;
        }

        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_closed) {
            return;
        }

        if (delay <= std::chrono::milliseconds::zero()) {
            m_ready_tasks.push(cb);
            lock.unlock();
            m_ready_cv.notify_one();
        } else {
            const auto due_time = std::chrono::steady_clock::now() + delay;
            const uint64_t seq = ++m_sequence_counter;
            const bool is_earliest = m_delayed_tasks.empty() || (due_time < m_delayed_tasks.top().due_time);

            m_delayed_tasks.push({due_time, seq, cb});
            lock.unlock();

            if (is_earliest) {
                m_timer_cv.notify_one();
            }
        }
    }

    void signalr_default_scheduler::internals::close() {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (m_closed) {
                return;
            }
            m_closed = true;

            // Immediately purge pending tasks to release captured resources promptly
            while (!m_ready_tasks.empty()) {
                m_ready_tasks.pop();
            }
            while (!m_delayed_tasks.empty()) {
                m_delayed_tasks.pop();
            }
        }

        m_timer_cv.notify_all();
        m_ready_cv.notify_all();
    }

    void signalr_default_scheduler::internals::timer_loop() {
        while (true) {
            size_t ready_count = 0;
            {
                std::unique_lock<std::mutex> lock(m_mutex);

                if (m_closed) {
                    return;
                }

                if (m_delayed_tasks.empty()) {
                    m_timer_cv.wait(lock, [this]() { return m_closed || !m_delayed_tasks.empty(); });
                } else {
                    const auto next_time = m_delayed_tasks.top().due_time;
                    m_timer_cv.wait_until(lock, next_time, [this, next_time]() {
                        return m_closed || m_delayed_tasks.empty() || m_delayed_tasks.top().due_time < next_time;
                    });
                }

                if (m_closed) {
                    return;
                }

                const auto now = std::chrono::steady_clock::now();
                while (!m_delayed_tasks.empty() && m_delayed_tasks.top().due_time <= now) {
                    auto task = std::move(const_cast<delayed_task&>(m_delayed_tasks.top()).callback);
                    m_delayed_tasks.pop();
                    m_ready_tasks.push(std::move(task));
                    ++ready_count;
                }
            }

            if (ready_count > 0) {
                if (ready_count == 1) {
                    m_ready_cv.notify_one();
                } else {
                    m_ready_cv.notify_all();
                }
            }
        }
    }

    void signalr_default_scheduler::internals::worker_loop() {
        while (true) {
            signalr_base_cb task;
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_ready_cv.wait(lock, [this]() { return m_closed || !m_ready_tasks.empty(); });

                if (m_ready_tasks.empty()) {
                    if (m_closed) {
                        return;
                    }
                    continue;
                }

                task = std::move(m_ready_tasks.front());
                m_ready_tasks.pop();

                // Cascade notification: if more tasks are queued, ensure sibling workers are awake
                if (!m_ready_tasks.empty()) {
                    m_ready_cv.notify_one();
                }
            }

            try {
                if (task) {
                    task();
                }
            } catch (...) {
                // Unhandled callback exceptions do not kill the worker thread
            }

            // Destruct task outside the mutex lock to prevent deadlocks if a captured object's
            // destructor schedules additional work.
            task = nullptr;
        }
    }

    signalr_default_scheduler::signalr_default_scheduler(size_t thread_count)
        : m_internals(std::make_shared<internals>(thread_count))
    {
        m_internals->start();
    }

    void signalr_default_scheduler::schedule(const signalr_base_cb& cb) {
        schedule(cb, std::chrono::milliseconds::zero());
    }

    void signalr_default_scheduler::schedule(const signalr_base_cb& cb, const std::chrono::milliseconds delay) {
        m_internals->schedule(cb, delay);
    }

    void signalr_default_scheduler::close() const {
        m_internals->close();
    }

    signalr_default_scheduler::~signalr_default_scheduler() {
        close();
    }

    void timer(const std::shared_ptr<scheduler>& scheduler, const std::function<bool(std::chrono::milliseconds)>& func) {
        timer_internal(scheduler, func, std::chrono::milliseconds::zero());
    }

    void timer_internal(const std::shared_ptr<scheduler>& scheduler,
                        const std::function<bool(std::chrono::milliseconds)>& func,
                        std::chrono::milliseconds duration)
    {
        constexpr auto tick = std::chrono::seconds(1);
        duration += tick;
        scheduler->schedule([func, scheduler, duration]() {
            if (!func(duration))
            {
                timer_internal(scheduler, func, duration);
            }
        }, tick);
    }
}
