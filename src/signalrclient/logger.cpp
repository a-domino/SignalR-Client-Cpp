// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "logger.h"
#include <iomanip>
#include <sstream>
#include <iostream>
#include <cassert>
#include <chrono>
#include <cstdarg>
#include <vector>

namespace signalr
{
#ifdef WIN32
#define GMTIME(r_tm, r_time_t) gmtime_s(r_tm, r_time_t)
#else
#define GMTIME(r_tm, r_time_t) gmtime_r(r_time_t, r_tm)
#endif

    logger::logger(
        const std::shared_ptr<log_writer>& log_writer, const trace_level trace_level,
        log_entry_handler log_entry_handler
    ) noexcept : m_log_writer(log_writer), m_trace_level(trace_level), m_log_entry_handler(std::move(log_entry_handler))
    { }

    void logger::log(const trace_level level, const std::string& entry) const
    {
        log(level, entry.data(), entry.length());
    }

    void logger::log(const trace_level level, const char* entry, const size_t entry_count) const
    {
        if (level < m_trace_level)
        {
            return;
        }
        if (m_log_entry_handler != nullptr)
        {
            try
            {
                m_log_entry_handler(level, entry, entry_count);
            }
            catch (const std::exception& e)
            {
                std::cerr << "error occurred in log_entry_handler: " << e.what() << std::endl;
            }
            catch (...)
            {
                std::cerr << "unknown error occurred in log_entry_handler" << std::endl;
            }
        }
        if (m_log_writer != nullptr)
        {
            try
            {
                std::chrono::milliseconds milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()
                );
                const std::chrono::seconds seconds = std::chrono::duration_cast<std::chrono::seconds>(milliseconds);
                milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(milliseconds - seconds);

                time_t t = seconds.count();
                tm time{};
                // convert time to utc
                GMTIME(&time, &t);

                char timeString[sizeof("2019-11-23T13:23:02.000Z")];

                // format string to ISO8601 with additional space for 3 digits of millisecond precision
                std::strftime(timeString, sizeof(timeString), "%FT%T.000Z", &time);

                // add millisecond part
                // 5 = 3 digits of millisecond precision + 'Z' + null character ending
                snprintf(timeString + sizeof(timeString) - 5, 5, "%03dZ", static_cast<int>(milliseconds.count()));

                std::stringstream os;
                os << timeString;

                write_trace_level(level, os);

                os.write(entry, static_cast<std::streamsize>(entry_count)) << std::endl;
                m_log_writer->write(os.str());
            }
            catch (const std::exception &e)
            {
                std::cerr << "error occurred when logging: " << e.what()
                    << std::endl << "    entry: ";
                std::cerr.write(entry, static_cast<std::streamsize>(entry_count)) << std::endl;
            }
            catch (...)
            {
                std::cerr << "unknown error occurred when logging" << std::endl
                          << "    entry: ";
                std::cerr.write(entry, static_cast<std::streamsize>(entry_count)) << std::endl;
            }
        }
    }

    void logger::logf(const trace_level level, const char* format, ...) const
    {
        if (!is_enabled(level))
        {
            return;
        }

        va_list args;
        va_start(args, format);

        char stack_buffer[1024];
        va_list args_copy;
        va_copy(args_copy, args);
        const int written = vsnprintf(stack_buffer, sizeof(stack_buffer), format, args_copy);
        va_end(args_copy);

        if (written < 0)
        {
            va_end(args);
            return;
        }

        const auto length = static_cast<size_t>(written);
        if (length < sizeof(stack_buffer))
        {
            log(level, static_cast<const char*>(stack_buffer), length);
        }
        else
        {
            // Allocate once only if entry exceeds stack buffer
            std::vector<char> heap_buffer(length + 1);
            vsnprintf(heap_buffer.data(), heap_buffer.size(), format, args);
            log(level, static_cast<const char*>(heap_buffer.data()), length);
        }

        va_end(args);
    }

    void logger::write_trace_level(const trace_level trace_level, std::ostream& stream)
    {
        switch (trace_level)
        {
        case signalr::trace_level::verbose:
            stream << " [verbose  ] ";
            break;
        case signalr::trace_level::debug:
            stream << " [debug    ] ";
            break;
        case signalr::trace_level::info:
            stream << " [info     ] ";
            break;
        case signalr::trace_level::warning:
            stream << " [warning  ] ";
            break;
        case signalr::trace_level::error:
            stream << " [error    ] ";
            break;
        case signalr::trace_level::critical:
            stream << " [critical ] ";
            break;
        case signalr::trace_level::none:
            stream << " [none     ] ";
            break;
        default:
            assert(false);
            stream << " [(unknown)] ";
            break;
        }
    }
}
