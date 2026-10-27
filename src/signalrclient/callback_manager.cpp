// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "callback_manager.h"
#include <sstream>

namespace signalr
{
    // dtor_clear_arguments will be passed when closing any pending callbacks when the `callback_manager` is
    // destroyed (i.e. in the dtor)
    callback_manager::callback_manager(const char* dtor_clear_arguments)
        : m_dtor_clear_arguments(dtor_clear_arguments)
    { }

    callback_manager::~callback_manager()
    {
        clear(m_dtor_clear_arguments.data());
    }

    // note: callback must not throw except for the `on_progress` callback which will never be invoked from the dtor
    std::string callback_manager::register_complete_callback(
        const std::function<void(const char* err_msg, value&&)>& callback
    )
    {
        auto callback_id = std::move(get_callback_id());

        {
            std::lock_guard<std::mutex> lock(m_map_lock);

            m_complete_callbacks.insert(std::make_pair(callback_id, callback));
        }

        return callback_id;
    }


    // invokes a callback and stops tracking it if remove callback set to true
    bool callback_manager::invoke_complete_callback(
        const std::string& callback_id, const char* error, signalr::value&& arguments, bool remove_callback
    )
    {
        std::function<void(const char*, signalr::value&& arguments)> callback;

        {
            std::lock_guard<std::mutex> lock(m_map_lock);

            auto iter = m_complete_callbacks.find(callback_id);
            if (iter == m_complete_callbacks.end())
            {
                return false;
            }

            callback = iter->second;

            if (remove_callback)
            {
                m_complete_callbacks.erase(callback_id);
            }
        }

        callback(error, std::move(arguments));
        return true;
    }

    bool callback_manager::remove_complete_callback(const std::string& callback_id)
    {
        {
            std::lock_guard<std::mutex> lock(m_map_lock);

            return m_complete_callbacks.erase(callback_id) != 0;
        }
    }

    void callback_manager::clear(const char* error)
    {
        {
            std::lock_guard<std::mutex> lock(m_map_lock);

            for (auto& kvp : m_complete_callbacks)
            {
                kvp.second(error, signalr::value());
            }

            m_complete_callbacks.clear();
        }
    }

    std::string callback_manager::get_callback_id()
    {
        return std::move(std::to_string(m_id++));
    }
}
