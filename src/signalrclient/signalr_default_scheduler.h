// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "../include/signalrclient/scheduler.h"

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

namespace signalr
{
    class signalr_default_scheduler : public scheduler
    {
    public:
        explicit signalr_default_scheduler(size_t thread_count = 4);
        signalr_default_scheduler(const signalr_default_scheduler&) = delete;
        signalr_default_scheduler& operator=(const signalr_default_scheduler&) = delete;

        void schedule(const signalr_base_cb& cb) override;
        void schedule(const signalr_base_cb& cb, std::chrono::milliseconds delay) override;
        ~signalr_default_scheduler() override;

        void close();

    private:
        struct delayed_task
        {
            std::chrono::steady_clock::time_point due_time;
            uint64_t sequence_id;
            signalr_base_cb callback;

            bool operator>(const delayed_task& other) const
            {
                if (due_time != other.due_time)
                {
                    return due_time > other.due_time;
                }
                return sequence_id > other.sequence_id;
            }
        };

        struct internals : public std::enable_shared_from_this<internals>
        {
            const size_t m_thread_count;
            std::mutex m_mutex;
            std::condition_variable m_ready_cv;
            std::condition_variable m_timer_cv;
            std::queue<signalr_base_cb> m_ready_tasks;
            std::priority_queue<delayed_task, std::vector<delayed_task>, std::greater<delayed_task>> m_delayed_tasks;
            bool m_closed{false};
            uint64_t m_sequence_counter{0};

            explicit internals(size_t thread_count);

            void start();
            void schedule(const signalr_base_cb& cb, std::chrono::milliseconds delay);
            void close();
            void timer_loop();
            void worker_loop();
        };

        std::shared_ptr<internals> m_internals;
    };

    void timer_internal(const std::shared_ptr<scheduler>& scheduler,
                        const std::function<bool(std::chrono::milliseconds)>& func,
                        std::chrono::milliseconds duration);

    void timer(const std::shared_ptr<scheduler>& scheduler,
               const std::function<bool(std::chrono::milliseconds)>& func);
}
