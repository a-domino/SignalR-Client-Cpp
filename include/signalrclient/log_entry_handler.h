// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.
//
// Created by austin on 10/23/26.
//

#ifndef MICROSOFT_SIGNALR_LOG_ENTRY_HANDLER_H
#define MICROSOFT_SIGNALR_LOG_ENTRY_HANDLER_H
#include <functional>

#include "trace_level.h"

namespace signalr
{
    typedef std::function<void(trace_level level, const char* entry, std::size_t entry_count)> log_entry_handler;
}

#endif //MICROSOFT_SIGNALR_LOG_ENTRY_HANDLER_H
