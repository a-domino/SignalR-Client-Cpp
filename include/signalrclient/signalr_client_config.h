// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "_exports.h"
#include <map>
#include <string>
#include "scheduler.h"
#include <memory>

namespace signalr
{
    class signalr_client_config
    {
    public:

        // TODO: re-implement features that were included when the Cpp Rest SDK was being used:
        //  - Set Proxy (although, a proxy getter/setter is already included in both configs...):
        //     + Old signature: void __cdecl set_proxy(const web::web_proxy &proxy);
        //  - Set Credentials (although, a set creds getter/setter is already included in both configs...):
        //     + Old signature: void __cdecl set_credentials(const web::credentials &credentials);
        //  - Set/Get HTTP Client Config:
        //     + Old Set Signature: void __cdecl set_http_client_config(const web::http::client::http_client_config& http_client_config);
        //     + Old Get Signature: web::http::client::http_client_config __cdecl get_http_client_config() const;
        //  - Set/Get Websocket Client Config:
        //     + Old Set Signature: void __cdecl set_websocket_client_config(const web::websockets::client::websocket_client_config& websocket_client_config);
        //     + Old Get Signature: web::websockets::client::websocket_client_config __cdecl get_websocket_client_config() const noexcept;

        // For the web::http::client::http_client_config, there isn't a direct translation into any 1 HTTP library, so
        // a new config will likely need to be defined that supports most, if not all, of the functionality that the old
        // config file provided.

        SIGNALRCLIENT_API signalr_client_config();

        SIGNALRCLIENT_API const std::map<std::string, std::string>& __cdecl get_http_headers() const noexcept;
        SIGNALRCLIENT_API std::map<std::string, std::string>& __cdecl get_http_headers() noexcept;
        SIGNALRCLIENT_API void __cdecl set_http_headers(const std::map<std::string, std::string>& http_headers);
        SIGNALRCLIENT_API void __cdecl set_scheduler(std::shared_ptr<scheduler> scheduler);
        SIGNALRCLIENT_API const std::shared_ptr<scheduler>& __cdecl get_scheduler() const noexcept;
        SIGNALRCLIENT_API void set_handshake_timeout(std::chrono::milliseconds);
        SIGNALRCLIENT_API std::chrono::milliseconds get_handshake_timeout() const noexcept;
        SIGNALRCLIENT_API void set_server_timeout(std::chrono::milliseconds);
        SIGNALRCLIENT_API std::chrono::milliseconds get_server_timeout() const noexcept;
        SIGNALRCLIENT_API void set_keepalive_interval(std::chrono::milliseconds);
        SIGNALRCLIENT_API std::chrono::milliseconds get_keepalive_interval() const noexcept;

    private:
        std::map<std::string, std::string> m_http_headers;
        std::shared_ptr<scheduler> m_scheduler;
        std::chrono::milliseconds m_handshake_timeout;
        std::chrono::milliseconds m_server_timeout;
        std::chrono::milliseconds m_keepalive_interval;
    };
}
