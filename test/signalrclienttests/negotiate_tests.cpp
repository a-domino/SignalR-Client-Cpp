// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#include "stdafx.h"
#include "negotiate.h"
#include "test_http_client.h"
#include "test_utils.h"
#include "signalr_default_scheduler.h"
#include "signalrclient/signalr_exception.h"

using namespace signalr;

TEST(negotiate, request_created_with_correct_url) {
    std::string requested_url;
    auto http_client = std::make_shared<test_http_client>(
        [&requested_url](const std::string& url, const http_request&, const cancellation_token&) -> http_response {
            std::string response_body(
                "{ \"connectionId\" : \"f7707523-307d-4cba-9abf-3eef701241e8\", "
                "\"availableTransports\" : [] }"
            );

            requested_url = url;
            return {200, response_body};
        }
    );

    http_client->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        http_client,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](signalr::negotiation_response&&, const std::exception_ptr&) { mre.set(); },
        get_cancellation_token(cts)
    );

    mre.get();
    ASSERT_EQ(
        "http://fake/signalr/negotiate?negotiateVersion=1",
        requested_url
    );
}

TEST(negotiate, negotiation_request_sent_and_response_serialized) {
    auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            std::string response_body(
                "{\"connectionId\" : \"f7707523-307d-4cba-9abf-3eef701241e8\", "
                "\"availableTransports\" : [ { \"transport\": \"WebSockets\", \"transferFormats\": [ \"Text\", \"Binary\" ] },"
                "{ \"transport\": \"ServerSentEvents\", \"transferFormats\": [ \"Text\" ] } ] }"
            );

            return {200, response_body};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<negotiation_response>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&& response, const std::exception_ptr&) {
            mre.set(std::move(response));
        },
        get_cancellation_token(cts)
    );

    auto response = mre.get();

    ASSERT_EQ("f7707523-307d-4cba-9abf-3eef701241e8", response.connectionId);
    ASSERT_EQ("f7707523-307d-4cba-9abf-3eef701241e8", response.connectionToken);
    ASSERT_EQ(2u, response.availableTransports.size());
    ASSERT_EQ(2u, response.availableTransports[0].transfer_formats.size());
    ASSERT_EQ("Text", response.availableTransports[0].transfer_formats[0]);
    ASSERT_EQ("Binary", response.availableTransports[0].transfer_formats[1]);
    ASSERT_EQ(1u, response.availableTransports[1].transfer_formats.size());
    ASSERT_EQ("Text", response.availableTransports[1].transfer_formats[0]);
}

TEST(negotiate, negotiation_response_with_redirect) {
    auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            std::string response_body(
                "{\"url\" : \"http://redirect\", "
                "\"accessToken\" : \"secret\" }"
            );
            return {200, response_body};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<negotiation_response>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&& response, const std::exception_ptr&) {
            mre.set(std::move(response));
        },
        get_cancellation_token(cts)
    );

    auto response = mre.get();

    ASSERT_EQ("http://redirect", response.url);
    ASSERT_EQ("secret", response.accessToken);
}

TEST(negotiate, negotiation_response_with_negotiateVersion) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            std::string response_body(
                "{\"connectionId\" : \"f7707523-307d-4cba-9abf-3eef701241e8\", "
                "\"negotiateVersion\": 1, "
                "\"connectionToken\": \"42\", "
                "\"availableTransports\" : [ { \"transport\": \"WebSockets\", \"transferFormats\": [ \"Text\", \"Binary\" ] },"
                "{ \"transport\": \"ServerSentEvents\", \"transferFormats\": [ \"Text\" ] } ] }"
            );
            return {200, response_body};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<negotiation_response>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&& response, const std::exception_ptr&) {
            mre.set(std::move(response));
        },
        get_cancellation_token(cts)
    );

    auto response = mre.get();

    ASSERT_EQ("42", response.connectionToken);
    ASSERT_EQ("f7707523-307d-4cba-9abf-3eef701241e8", response.connectionId);
}

TEST(negotiate, negotiation_response_with_future_negotiateVersion) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            std::string response_body(
                "{\"connectionId\" : \"f7707523-307d-4cba-9abf-3eef701241e8\", "
                "\"negotiateVersion\": 99, "
                "\"connectionToken\": \"42\", "
                "\"availableTransports\" : [ { \"transport\": \"WebSockets\", \"transferFormats\": [ \"Text\", \"Binary\" ] },"
                "{ \"transport\": \"ServerSentEvents\", \"transferFormats\": [ \"Text\" ] } ] }"
            );
            return {200, response_body};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<negotiation_response>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&& response, const std::exception_ptr&) {
            mre.set(std::move(response));
        },
        get_cancellation_token(cts)
    );

    const auto response = mre.get();

    ASSERT_EQ("42", response.connectionToken);
    ASSERT_EQ("f7707523-307d-4cba-9abf-3eef701241e8", response.connectionId);
}

TEST(negotiate, negotiation_response_with_error_string_sets_error_member) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            return {200, R"({"error": "Server is overloaded or unauthorized"})"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<negotiation_response>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&& response, const std::exception_ptr& exception) {
            ASSERT_EQ(nullptr, exception);
            mre.set(std::move(response));
        },
        get_cancellation_token(cts)
    );

    const auto response = mre.get();
    ASSERT_EQ("Server is overloaded or unauthorized", response.error);
}

TEST(negotiate, negotiation_fails_on_non_200_status_code) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            return {500, "Internal Server Error"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&, const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get();
        FAIL();
    } catch (const signalr_exception& ex) {
        ASSERT_STREQ(
            "negotiate failed with status code 500",
            ex.what()
        );
    }
}

TEST(negotiate, negotiation_fails_when_client_send_throws) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            throw std::runtime_error("network connection dropped");
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&,const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get();
        FAIL();
    } catch (const std::runtime_error& ex) {
        ASSERT_STREQ(
            "network connection dropped",
            ex.what()
        );
    }
}

TEST(negotiate, negotiation_fails_when_cancelled) {
    const auto cts = std::make_shared<cancellation_token_source>();
    cts->cancel();

    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            return {200, R"({"connectionId": "123"})"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&, const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get(); // should throw
        FAIL();
    } catch (const canceled_exception&) {
        SUCCEED();
    }
}

TEST(negotiate, negotiation_rejects_asp_net_legacy_signalr_server) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            return {200, R"({"Url":"/signalr","ConnectionToken":"abc","ProtocolVersion":"1.3"})"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&, const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get(); // should throw.
        FAIL();
    } catch (const signalr_exception& ex) {
        ASSERT_TRUE(
            std::string(ex.what()).find("Detected a connection attempt to an ASP.NET SignalR Server") !=
            std::string::npos
        );
    }
}

TEST(negotiate, negotiation_fails_on_malformed_json_response) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            return {200, "{ invalid json structure"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&, const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get();
        FAIL();
    } catch (...) {}
}

TEST(negotiate, negotiation_fails_on_non_object_json_response) {
    const auto request_factory = std::make_shared<test_http_client>(
        [](const std::string&, const http_request&, const cancellation_token&) -> http_response {
            return {200, R"(["not", "an", "object"])"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&, const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get(); // should throw
        FAIL();
    } catch (const signalr_exception& ex) {
        ASSERT_STREQ(
            "negotiation response is not a valid JSON object",
            ex.what()
        );
    }
}

TEST(negotiate, custom_headers_are_attached_to_http_request) {
    std::map<std::string, std::string> observed_headers;
    const auto request_factory = std::make_shared<test_http_client>(
        [&observed_headers]
        (const std::string&, const http_request& request, const cancellation_token&) -> http_response {
            observed_headers = request.headers;
            return {200, R"({"connectionId": "test"})"};
        }
    );

    request_factory->set_scheduler(std::make_shared<signalr_default_scheduler>());

    signalr_client_config config;
    std::map<std::string, std::string> custom_headers = {
        {"X-Custom-Auth", "Bearer secret123"},
        {"X-Client-Type", "CppTest"}
    };
    config.set_http_headers(custom_headers);

    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        request_factory,
        "http://fake/signalr",
        config,
        [&mre](negotiation_response&&,const std::exception_ptr&) { mre.set(); },
        get_cancellation_token(cts)
    );

    mre.get();
    ASSERT_EQ("Bearer secret123", observed_headers["X-Custom-Auth"]);
    ASSERT_EQ("CppTest", observed_headers["X-Client-Type"]);
}

TEST(negotiate, null_http_client_invokes_callback_with_exception) {
    const auto cts = std::make_shared<cancellation_token_source>();
    auto mre = manual_reset_event<void>();
    negotiate::negotiate(
        nullptr,
        "http://fake/signalr",
        signalr_client_config(),
        [&mre](negotiation_response&&, const std::exception_ptr& exception) {
            mre.set(exception);
        },
        get_cancellation_token(cts)
    );

    try {
        mre.get(); // should throw.
        FAIL();
    } catch (...) { }
}