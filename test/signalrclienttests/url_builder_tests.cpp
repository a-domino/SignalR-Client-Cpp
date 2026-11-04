// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "url_builder.h"

using namespace signalr;

TEST(url_builder_negotiate, url_correct_if_query_string_empty) {
    ASSERT_EQ(
        "http://fake/negotiate",
        url_builder::build_negotiate("http://fake/")
    );
}

TEST(url_builder_negotiate, url_correct_if_query_string_not_empty) {
    ASSERT_EQ(
        "http://fake/negotiate?q1=1&q2=2",
        url_builder::build_negotiate("http://fake/?q1=1&q2=2")
    );
}

TEST(url_builder_negotiate, handles_path_without_trailing_slash) {
    ASSERT_EQ(
        "http://fake/hub/negotiate",
        url_builder::build_negotiate("http://fake/hub")
    );
}

TEST(url_builder_negotiate, handles_path_with_trailing_slash) {
    ASSERT_EQ(
        "http://fake/hub/negotiate",
        url_builder::build_negotiate("http://fake/hub/")
    );
}

TEST(url_builder_connect_webSockets, url_correct_if_query_string_not_empty) {
    ASSERT_EQ(
        "ws://fake/?q1=1&q2=2",
        url_builder::build_connect(
            "http://fake/",
            transport_type::websockets,
            "q1=1&q2=2"
        )
    );
}

TEST(url_builder_connect, url_correct_if_query_string_not_empty_and_adding_query_string) {
    ASSERT_EQ(
        "ws://fake/?q=0&q1=1&q2=2",
        url_builder::build_connect(
            "http://fake/?q=0",
            transport_type::websockets,
            "q1=1&q2=2"
        )
    );
}

TEST(url_builder_connect, converts_https_to_wss) {
    ASSERT_EQ(
        "wss://secure.fake/chat",
        url_builder::build_connect(
            "https://secure.fake/chat",
            transport_type::websockets,
            ""
        )
    );
}

TEST(url_builder_connect, preserves_existing_wss_scheme) {
    ASSERT_EQ(
        "wss://secure.fake/chat",
        url_builder::build_connect(
            "wss://secure.fake/chat",
            transport_type::websockets,
            ""
        )
    );
}

TEST(url_builder_connect, preserves_existing_ws_scheme) {
    ASSERT_EQ(
        "ws://plain.fake/chat",
        url_builder::build_connect(
            "ws://plain.fake/chat",
            transport_type::websockets,
            ""
        )
    );
}

TEST(url_builder_connect, long_polling_keeps_http_scheme) {
    ASSERT_EQ(
        "http://fake/chat?q=1",
        url_builder::build_connect(
            "http://fake/chat",
            transport_type::long_polling,
            "q=1"
        )
    );
}

TEST(url_builder_connect, long_polling_keeps_https_scheme) {
    ASSERT_EQ(
        "https://fake/chat?q=1",
        url_builder::build_connect(
            "https://fake/chat",
            transport_type::long_polling,
            "q=1"
        )
    );
}

TEST(url_builder_add_query_string, merges_when_query_already_exists) {
    ASSERT_EQ(
        "http://fake/?existing=val&added=val2",
        url_builder::add_query_string(
            "http://fake/?existing=val",
            "added=val2"
        )
    );
}

TEST(url_builder_add_query_string, handles_leading_question_mark_cleanly) {
    ASSERT_EQ(
        "http://fake/?param=value",
        url_builder::add_query_string(
            "http://fake/",
            "?param=value"
        )
    );
}