// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "test_utils.h"
#include "../src/signalrclient/signalr_default_scheduler.h"
#include <atomic>
#include <chrono>
#include <set>
#include <vector>

using namespace signalr;

// --- Existing Microsoft SignalR Client Tests ---

TEST(scheduler, callbacks_run_on_different_thread)
{
    signalr_default_scheduler scheduler;

    auto current_thread = std::this_thread::get_id();
    std::thread::id id;
    auto mre = manual_reset_event<void>();
    scheduler.schedule([&id, &mre]()
        {
            id = std::this_thread::get_id();
            mre.set();
        });

    mre.get();
    ASSERT_NE(current_thread, id);
}

TEST(scheduler, callback_can_be_called_when_delayed_callback_called_first)
{
    signalr_default_scheduler scheduler;

    auto mre = manual_reset_event<void>();
    auto timer_mre = manual_reset_event<void>();
    bool first_callback = false;
    scheduler.schedule([&first_callback, &timer_mre]()
        {
            first_callback = true;
            timer_mre.set();
        }, std::chrono::milliseconds(1000));

    scheduler.schedule([&mre]()
        {
            mre.set();
        });

    mre.get();
    ASSERT_FALSE(first_callback);
    timer_mre.get();
}

TEST(scheduler, callback_with_delay_is_delayed)
{
    signalr_default_scheduler scheduler;
    auto delay = std::chrono::milliseconds(100);

    auto mre = manual_reset_event<void>();
    auto prev_now = std::chrono::steady_clock::now();
    scheduler.schedule([&mre]()
        {
            mre.set();
        }, delay);

    mre.get();

    auto now = std::chrono::steady_clock::now();
    ASSERT_TRUE(now - prev_now >= delay);
}

TEST(scheduler, scheduler_can_destruct_with_callbacks_registered)
{
    {
        signalr_default_scheduler scheduler;

        scheduler.schedule([]()
            {
            }, std::chrono::seconds(1));
    }

    manual_reset_event<void> start_mre{};
    manual_reset_event<void> continue_mre{};
    {
        signalr_default_scheduler scheduler;

        scheduler.schedule([&start_mre, &continue_mre]()
            {
                start_mre.set();
                continue_mre.get();

                // avoids referencing these objects after they've been destructed
                start_mre.set();
            });

        start_mre.get();
    }
    continue_mre.set();
    start_mre.get();
}

// --- Additional Coverage Tests ---

TEST(scheduler, immediate_callbacks_execute_concurrently_across_workers)
{
    constexpr size_t worker_count = 2;
    signalr_default_scheduler scheduler(worker_count);

    std::thread::id thread1_id;
    std::thread::id thread2_id;
    std::atomic<bool> thread2_saw_thread1_running{false};
    std::atomic<bool> thread1_is_running{false};

    manual_reset_event<void> t1_started;
    manual_reset_event<void> t2_started;
    manual_reset_event<void> release_t1;
    manual_reset_event<void> release_t2;
    manual_reset_event<void> t1_done;
    manual_reset_event<void> t2_done;

    // Schedule Task 1, which signals that it started and blocks until released
    scheduler.schedule([&]() {
        thread1_id = std::this_thread::get_id();
        thread1_is_running.store(true);
        t1_started.set();

        release_t1.get();

        thread1_is_running.store(false);
        t1_done.set();
    });

    // Wait until Task 1 is actively running inside Worker 1
    t1_started.get();

    // Schedule Task 2 while Task 1 is still blocked
    scheduler.schedule([&]() {
        thread2_id = std::this_thread::get_id();

        // Verify concurrency: Task 1 must still be active while Task 2 executes
        if (thread1_is_running.load())
        {
            thread2_saw_thread1_running.store(true);
        }

        t2_started.set();

        release_t2.get();

        t2_done.set();
    });

    // Wait until Task 2 has started alongside Task 1
    t2_started.get();

    // Assert that both tasks ran simultaneously on different threads
    ASSERT_TRUE(thread2_saw_thread1_running.load());
    ASSERT_NE(thread1_id, thread2_id);
    ASSERT_NE(std::this_thread::get_id(), thread1_id);
    ASSERT_NE(std::this_thread::get_id(), thread2_id);

    // Release both callbacks and await clean completion
    release_t1.set();
    release_t2.set();

    t1_done.get();
    t2_done.get();
}

TEST(scheduler, fast_delayed_callbacks_do_not_wait_for_15ms_polling_interval)
{
    signalr_default_scheduler scheduler(2);
    manual_reset_event<void> mre;
    const auto start = std::chrono::steady_clock::now();

    scheduler.schedule([&mre]() {
        mre.set();
    }, std::chrono::milliseconds(2));

    mre.get();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    // The old implementation woke on a 15ms wait_for loop; the new wait_until fires in < 14ms
    ASSERT_LT(elapsed, 14);
}

TEST(scheduler, reentrant_schedule_does_not_deadlock)
{
    auto scheduler = std::make_shared<signalr_default_scheduler>(2);
    manual_reset_event<void> mre;
    std::atomic<int> value{0};

    scheduler->schedule([scheduler, &value, &mre]() {
        scheduler->schedule([&value, &mre]() {
            value = 42;
            mre.set();
        });
    });

    mre.get();
    ASSERT_EQ(42, value.load());
}

TEST(scheduler, unhandled_callback_exception_does_not_terminate_worker)
{
    signalr_default_scheduler scheduler(1);
    manual_reset_event<void> mre;
    bool second_callback_ran = false;

    scheduler.schedule([]() {
        throw std::runtime_error("Simulated failure");
    });

    scheduler.schedule([&second_callback_ran, &mre]() {
        second_callback_ran = true;
        mre.set();
    });

    mre.get();
    ASSERT_TRUE(second_callback_ran);
}

TEST(scheduler, registered_callbacks_are_not_invoked_after_destruction)
{
    std::atomic<bool> ran{false};
    {
        signalr_default_scheduler scheduler;
        scheduler.schedule([&ran]() {
            ran = true;
        }, std::chrono::milliseconds(50));
    } // Scheduler destructed immediately

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(ran.load());
}

TEST(scheduler, timer_runs_periodically_until_stopped)
{
    auto scheduler = std::make_shared<signalr_default_scheduler>(2);
    manual_reset_event<void> mre;
    std::atomic<int> tick_count{0};

    timer(scheduler, [&](std::chrono::milliseconds) -> bool {
        if (++tick_count >= 3)
        {
            mre.set();
            return true;
        }
        return false;
    });

    mre.get();
    ASSERT_EQ(3, tick_count.load());
}