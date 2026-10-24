// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "test_utils.h"
#include "logger.h"
#include "memory_log_writer.h"
#include <regex>
#include <string>
#include <vector>
#include <stdexcept>

using namespace signalr;

namespace
{
    // Mock writer that throws to verify logger exception isolation
    class throwing_log_writer : public log_writer
    {
    public:
        void write(const std::string&) override
        {
            throw std::runtime_error("simulated disk/network write error");
        }
    };

    struct log_record
    {
        trace_level level;
        std::string message;
    };
}

// ============================================================================
// 1. Trace Level & Writer Basic Tests (Preserved & Extended)
// ============================================================================

TEST(logger_write, entry_added_if_trace_level_set)
{
    std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());

    logger l(writer, trace_level::info);
    l.log(trace_level::info, "message");

    auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();

    ASSERT_EQ(1U, log_entries.size());
}

TEST(logger_write, entry_not_added_if_trace_level_not_high_enough)
{
    std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());

    logger l(writer, trace_level::info);
    l.log(trace_level::debug, "event");

    auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();

    ASSERT_TRUE(log_entries.empty());
}

TEST(logger_write, entries_formatted_correctly)
{
    const std::shared_ptr<log_writer> writer(std::make_shared<memory_log_writer>());

    logger l(writer, trace_level::verbose);
    l.log(trace_level::info, "message");

    const auto log_entries = std::dynamic_pointer_cast<memory_log_writer>(writer)->get_log_entries();
    ASSERT_FALSE(log_entries.empty());

    const auto& entry = log_entries[0];

    // 0000-0-00T00:00:00.000Z [info     ] message\n
    const std::regex r{ R"([\d]{4}-[\d]{2}-[\d]{2}T[\d]{2}:[\d]{2}:[\d]{2}.[\d]{3}Z \[info     \] message\n)" };
    ASSERT_TRUE(std::regex_match(entry, r));

    const auto expected_message = std::string("[info     ] message\n");
    const auto pos = entry.find(expected_message);
    ASSERT_EQ(expected_message.size(), entry.size() - pos);
}

TEST(logger_write, writes_all_trace_level_tags)
{
    const auto writer = std::make_shared<memory_log_writer>();
    logger l(writer, trace_level::verbose);

    l.log(trace_level::verbose, "msg");
    l.log(trace_level::debug, "msg");
    l.log(trace_level::warning, "msg");
    l.log(trace_level::error, "msg");
    l.log(trace_level::critical, "msg");

    const auto entries = writer->get_log_entries();
    ASSERT_EQ(5U, entries.size());

    EXPECT_NE(std::string::npos, entries[0].find(" [verbose  ] msg\n"));
    EXPECT_NE(std::string::npos, entries[1].find(" [debug    ] msg\n"));
    EXPECT_NE(std::string::npos, entries[2].find(" [warning  ] msg\n"));
    EXPECT_NE(std::string::npos, entries[3].find(" [error    ] msg\n"));
    EXPECT_NE(std::string::npos, entries[4].find(" [critical ] msg\n"));
}

TEST(logger_write, std_string_overload_logs_identically)
{
    const auto writer = std::make_shared<memory_log_writer>();
    logger l(writer, trace_level::info);

    const std::string text = "dynamic message";
    l.log(trace_level::info, text);

    const auto entries = writer->get_log_entries();
    ASSERT_EQ(1U, entries.size());
    EXPECT_NE(std::string::npos, entries[0].find("dynamic message\n"));
}

TEST(logger_write, writer_exception_is_safely_caught)
{
    const auto writer = std::make_shared<throwing_log_writer>();
    logger l(writer, trace_level::info);

    EXPECT_NO_THROW(l.log(trace_level::info, "this will fail to write"));
}

// ============================================================================
// 2. is_enabled Tests
// ============================================================================

TEST(logger_enabled, evaluates_trace_level_thresholds_correctly)
{
    logger l(nullptr, trace_level::warning);

    EXPECT_FALSE(l.is_enabled(trace_level::verbose));
    EXPECT_FALSE(l.is_enabled(trace_level::debug));
    EXPECT_FALSE(l.is_enabled(trace_level::info));
    EXPECT_TRUE(l.is_enabled(trace_level::warning));
    EXPECT_TRUE(l.is_enabled(trace_level::error));
    EXPECT_TRUE(l.is_enabled(trace_level::critical));
}

// ============================================================================
// 3. log_entry_handler Tests
// ============================================================================

TEST(logger_handler, forwards_raw_entry_and_count)
{
    std::vector<log_record> records;
    auto handler = [&](trace_level level, const char* entry, size_t entry_count) {
        records.push_back({ level, std::string(entry, entry_count) });
    };

    logger l(nullptr, trace_level::info, handler);
    l.log(trace_level::info, "raw entry test");

    ASSERT_EQ(1U, records.size());
    EXPECT_EQ(trace_level::info, records[0].level);
    EXPECT_EQ("raw entry test", records[0].message);
}

TEST(logger_handler, not_called_if_level_is_below_threshold)
{
    int call_count = 0;
    auto handler = [&](trace_level, const char*, size_t) {
        call_count++;
    };

    logger l(nullptr, trace_level::warning, handler);
    l.log(trace_level::info, "ignored event");

    EXPECT_EQ(0, call_count);
}

TEST(logger_handler, dispatches_to_both_writer_and_handler)
{
    const auto writer = std::make_shared<memory_log_writer>();
    std::string captured_handler_msg;

    auto handler = [&](trace_level, const char* entry, size_t count) {
        captured_handler_msg.assign(entry, count);
    };

    logger l(writer, trace_level::info, handler);
    l.log(trace_level::info, "dual dispatch");

    const auto writer_entries = writer->get_log_entries();
    ASSERT_EQ(1U, writer_entries.size());
    EXPECT_NE(std::string::npos, writer_entries[0].find("dual dispatch\n"));

    EXPECT_EQ("dual dispatch", captured_handler_msg);
}

TEST(logger_handler, catches_std_exception_without_terminating)
{
    auto handler = [](trace_level, const char*, size_t) {
        throw std::runtime_error("handler pipeline error");
    };

    logger l(nullptr, trace_level::info, handler);
    EXPECT_NO_THROW(l.log(trace_level::info, "exception test"));
}

TEST(logger_handler, catches_unknown_exception_without_terminating)
{
    auto handler = [](trace_level, const char*, size_t) {
        throw 42; // Non-std::exception NOLINT(*-exception-baseclass)
    };

    logger l(nullptr, trace_level::info, handler);
    EXPECT_NO_THROW(l.log(trace_level::info, "unknown exception test"));
}

// ============================================================================
// 4. Variadic Multi-Piece Concatenation Tests
// ============================================================================

TEST(logger_variadic, concatenates_mixed_types_without_formatting)
{
    std::string captured;
    auto handler = [&](trace_level, const char* entry, size_t count) {
        captured.assign(entry, count);
    };

    logger l(nullptr, trace_level::info, handler);

    const std::string str = "dynamic";
    const char* raw_ptr = "raw";
    l.log(trace_level::info, "literal: ", str, " and ", raw_ptr, " tail");

    EXPECT_EQ("literal: dynamic and raw tail", captured);
}

TEST(logger_variadic, respects_trace_level_filter)
{
    int call_count = 0;
    auto handler = [&](trace_level, const char*, size_t) {
        call_count++;
    };

    logger l(nullptr, trace_level::error, handler);
    l.log(trace_level::info, "part1", "part2");

    EXPECT_EQ(0, call_count);
}

TEST(logger_variadic, handles_payloads_exceeding_stack_buffer)
{
    std::string captured;
    auto handler = [&](trace_level, const char* entry, size_t count) {
        captured.assign(entry, count);
    };

    logger l(nullptr, trace_level::info, handler);

    // Create 2 parts whose combined length is 1400 bytes (> 1024-byte stack capacity)
    const std::string part1(700, 'A');
    const std::string part2(700, 'B');
    l.log(trace_level::info, part1, part2);

    ASSERT_EQ(1400U, captured.size());
    EXPECT_EQ(part1 + part2, captured);
}

// ============================================================================
// 5. Variadic logf (printf-style) Tests
// ============================================================================

TEST(logger_logf, formats_primitives_correctly)
{
    std::string captured;
    auto handler = [&](trace_level, const char* entry, size_t count) {
        captured.assign(entry, count);
    };

    logger l(nullptr, trace_level::info, handler);
    l.logf(trace_level::info, "Status: %d, Response: %s", 200, "OK");

    EXPECT_EQ("Status: 200, Response: OK", captured);
}

TEST(logger_logf, respects_trace_level_filter)
{
    int call_count = 0;
    auto handler = [&](trace_level, const char*, size_t) {
        call_count++;
    };

    logger l(nullptr, trace_level::error, handler);
    l.logf(trace_level::debug, "Code: %d", 123);

    EXPECT_EQ(0, call_count);
}

TEST(logger_logf, handles_payloads_exceeding_stack_buffer)
{
    std::string captured;
    auto handler = [&](trace_level, const char* entry, size_t count) {
        captured.assign(entry, count);
    };

    logger l(nullptr, trace_level::info, handler);

    // Build a large argument (> 1024 bytes) to force the heap reallocation branch
    const std::string large_string(1500, 'Z');
    l.logf(trace_level::info, "Header: %s", large_string.c_str());

    const std::string expected = "Header: " + large_string;
    ASSERT_EQ(expected.size(), captured.size());
    EXPECT_EQ(expected, captured);
}