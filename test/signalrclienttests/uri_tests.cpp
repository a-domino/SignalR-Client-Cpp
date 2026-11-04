// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "base_uri.h"

using namespace signalr;

TEST(uri_split_query, parses_both_ampersand_and_semicolon_delimiters) {
    auto result = uri::split_query("k1=v1;k2=v2&k3=v3");

    ASSERT_EQ(3u, result.size());
    ASSERT_EQ("v1", result["k1"]);
    ASSERT_EQ("v2", result["k2"]);
    ASSERT_EQ("v3", result["k3"]);
}

TEST(uri_split_query, handles_empty_values_correctly) {
    auto result = uri::split_query("k1=&k2=v2");

    ASSERT_EQ(2u, result.size());
    ASSERT_EQ("", result["k1"]);
    ASSERT_EQ("v2", result["k2"]);
}

TEST(uri_split_query, ignores_leading_question_mark) {
    auto result = uri::split_query("?k1=v1&k2=v2");

    ASSERT_EQ(2u, result.size());
    ASSERT_EQ("v1", result["k1"]);
    ASSERT_EQ("v2", result["k2"]);
}

TEST(uri_resolve_uri, correctly_removes_dot_segments) {
    uri base("http://fake.com/a/b/c");
    auto resolved = base.resolve_uri("../d");

    ASSERT_EQ("http://fake.com/a/d", resolved);
}

TEST(uri_resolve_uri, absolute_path_resolves_against_authority) {
    uri base("http://fake.com/a/b/c");
    auto resolved = base.resolve_uri("/root/path");

    ASSERT_EQ("http://fake.com/root/path", resolved);
}
