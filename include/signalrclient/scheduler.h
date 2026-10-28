// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include <exception>
#include <functional>
#include <chrono>

namespace signalr
{
    typedef std::function<void()> signalr_base_cb;

    struct scheduler
    {
        /**
         *
         * @param cb Callback to run
         */
        virtual void schedule(const signalr_base_cb& cb) = 0;
        virtual void schedule(const signalr_base_cb& cb, std::chrono::milliseconds delay) = 0;

        virtual ~scheduler() = default;
    };
}
