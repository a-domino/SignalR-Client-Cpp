// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "hub_callback_manager.h"

namespace signalr
{
    // dtor_clear_arguments will be passed when closing any pending callbacks when the `callback_manager` is
    // destroyed (i.e. in the dtor)
    hub_callback_manager::hub_callback_manager(const char* dtor_clear_arguments)
        : m_dtor_clear_arguments(dtor_clear_arguments)
    { }

    hub_callback_manager::~hub_callback_manager()
    {
        clear(m_dtor_clear_arguments.data());
    }

    // note: callback must not throw except for the `on_progress` callback which will never be invoked from the dtor
    std::string hub_callback_manager::register_complete_callback(
        const std::function<void(const char* err_msg, value&&)>& callback
    )
    {
        auto invocation_id = std::move(get_invocation_id());
        {
            std::lock_guard<std::mutex> lock(m_map_lock);
            m_complete_callbacks.insert(std::make_pair(invocation_id, callback));
        }
        return std::move(invocation_id);
    }

    std::string hub_callback_manager::register_stream_callbacks(
        const std::function<void(value&&)>& stream_item_callback,
        const std::function<void(const char*, value&&)>& complete_callback
    ) {
        auto callback_id = std::move(get_invocation_id());
        {
            std::lock_guard<std::mutex> lock(m_map_lock);
            m_handle_item_callbacks.insert(std::make_pair(callback_id, stream_item_callback));
            m_complete_callbacks.insert(std::make_pair(callback_id, complete_callback));
        }
        return std::move(callback_id);
    }

    bool hub_callback_manager::handle_stream_item_callback(const std::string& invocation_id, value&& item) const {
        std::function<void(value&&)> callback;
        {
            std::lock_guard<std::mutex> lock(m_map_lock);

            const auto iter = m_handle_item_callbacks.find(invocation_id);
            if (iter == m_handle_item_callbacks.cend())
            {
                return false;
            }

            callback = iter->second;
        }
        callback(std::move(item));
        return true;
    }

    bool hub_callback_manager::has_complete_callback_for(const std::string& invocation_id) const {
        std::lock_guard<std::mutex> lock(m_map_lock);
        return m_complete_callbacks.find(invocation_id) != m_complete_callbacks.cend();
    }

    // invokes a callback and stops tracking it if remove callback set to true
    bool hub_callback_manager::invoke_complete_callback(
        const std::string& invocation_id, const char* error, signalr::value&& result, const bool remove_callback
    ) {
        std::function<void(const char*, signalr::value&& result)> callback;
        {
            std::lock_guard<std::mutex> lock(m_map_lock);

            const auto iter = m_complete_callbacks.find(invocation_id);
            if (iter == m_complete_callbacks.cend())
            {
                return false;
            }

            callback = iter->second;

            if (remove_callback)
            {
                m_handle_item_callbacks.erase(invocation_id);
                m_complete_callbacks.erase(invocation_id);
            }
        }
        callback(error, std::move(result));
        return true;
    }

    bool hub_callback_manager::remove_complete_callback(const std::string& invocation_id)
    {
        {
            std::lock_guard<std::mutex> lock(m_map_lock);
            m_handle_item_callbacks.erase(invocation_id);
            return m_complete_callbacks.erase(invocation_id) != 0;
        }
    }

    void hub_callback_manager::clear(const char* error)
    {
        std::unordered_map<std::string, std::function<void(const char*, signalr::value&&)>> complete_callbacks;
        {
            std::lock_guard<std::mutex> lock(m_map_lock);
            m_handle_item_callbacks.clear();
            m_complete_callbacks.swap(complete_callbacks);
        }
        for (auto& kvp : complete_callbacks)
        {
            kvp.second(error, signalr::value());
        }
    }

    std::string hub_callback_manager::get_invocation_id()
    {
        return std::move(std::to_string(m_id++));
    }
}
