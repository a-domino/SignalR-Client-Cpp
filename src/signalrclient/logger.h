// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include <cstring>
#include <memory>

#include "signalrclient/log_entry_handler.h"
#include "signalrclient/trace_level.h"
#include "signalrclient/log_writer.h"

namespace signalr
{
    class logger
    {
        struct string_piece;

    public:
        logger(const std::shared_ptr<log_writer>& log_writer, trace_level trace_level, log_entry_handler = nullptr) noexcept;

        void log(trace_level level, const char* entry, size_t entry_count) const;

        // TODO: variadic 'const char*' overload to avoid std::string allocations
        void log(trace_level level, const std::string& entry) const;

        template <size_t N>
        void log(const trace_level level, const char (&entry)[N]) const
        {
            // remove '\0'
            log(level, entry, N - 1);
        }

        // Variadic multi-segment overload:
        // Concatenates 2 or more string-like arguments into a stack buffer without heap allocation.
        template <typename T1, typename T2, typename... TRest>
        void logv(const trace_level level, const T1& first, const T2& second, const TRest&... rest) const
        {
            if (!is_enabled(level))
            {
                return;
            }

            const string_piece pieces[] = { first, second, rest... };
            constexpr size_t count = sizeof...(rest) + 2;

            size_t total_length = 0;
            for (size_t i = 0; i < count; ++i)
            {
                total_length += pieces[i].size;
            }

            // Stack buffer handles the vast majority of log messages without heap allocation
            constexpr size_t stack_capacity = 1024;
            if (total_length < stack_capacity)
            {
                char buffer[stack_capacity];
                char* dest = buffer;
                for (size_t i = 0; i < count; ++i)
                {
                    if (pieces[i].size > 0)
                    {
                        std::memcpy(dest, pieces[i].data, pieces[i].size);
                        dest += pieces[i].size;
                    }
                }
                log(level, buffer, total_length);
            }
            else
            {
                // Fallback for unusually large log entries
                std::string buffer;
                buffer.reserve(total_length);
                for (size_t i = 0; i < count; ++i)
                {
                    if (pieces[i].size > 0)
                    {
                        buffer.append(pieces[i].data, pieces[i].size);
                    }
                }
                log(level, buffer.data(), total_length);
            }
        }

        // Optional printf-style variadic overload (implemented in logger.cpp)
        void logf(trace_level level, const char* format, ...) const;

        [[nodiscard]] bool is_enabled(const trace_level level) const
        {
            return level >= m_trace_level;
        }

    private:
        // Lightweight non-allocating string view helper (C++11 compatible)
        struct string_piece
        {
            const char* data;
            size_t size;

            string_piece(const char* str)
                : data(str), size(str ? std::strlen(str) : 0) {}

            template <size_t N>
            string_piece(const char (&str)[N])
                : data(str), size(N > 0 && str[N - 1] == '\0' ? N - 1 : N) {}

            string_piece(const std::string& str)
                : data(str.data()), size(str.size()) {}
        };

        std::shared_ptr<log_writer> m_log_writer;
        trace_level m_trace_level;
        log_entry_handler m_log_entry_handler;

        static void write_trace_level(trace_level trace_level, std::ostream& stream);
    };
}
