// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "signalrclient/signalr_value.h"
#include "hub_protocol.h"
#include <memory>
#include <string>
#include <tuple>
#include <map>
#include <stdexcept>
#include <utility>
#include <json/writer.h>

#include "json_helpers.h"
#include "signalrclient/signalr_exception.h"

namespace signalr
{
    namespace handshake
    {
        /**
         * Represents a parsed response message received from the SignalR server during the handshake phase.
         */
        struct handshake_response
        {
            /**
             * The optional error message sent by the server if the handshake was rejected.
             * If empty, the handshake succeeded.
             */
            std::string error;

            /**
             * The optional protocol minor version supported by the server.
             */
            int minor_version{0};

            /**
             * The raw parsed JSON properties returned in the handshake message.
             */
            signalr::value raw_message;

            /**
             * Determines whether the server reported a handshake failure.
             *
             * @return True if an error is present; otherwise false.
             */
            bool has_error() const noexcept {
                return !error.empty();
            }
        };

        /**
         * Serializes a SignalR handshake request message for the specified hub protocol.
         *
         * @param protocol The hub protocol instance containing the protocol name and target version.
         * @return A JSON-formatted string terminated with the Record Separator (0x1E).
         */
        inline std::string write_handshake(
            const hub_protocol& protocol
        ) {
            auto map = std::map<std::string, signalr::value>{
                {"protocol", signalr::value(protocol.name())},
                {"version", signalr::value(static_cast<double>(protocol.version()))}
            };

            return Json::writeString(
                getJsonWriter(),
                createJson(signalr::value(std::move(map)))
            ) + JSON_MSG_SEP;
        }

        /**
         * Serializes a SignalR handshake request message for the hub protocol managed by a unique_ptr.
         *
         * @param protocol A unique_ptr owning the hub protocol instance. Must not be null.
         * @return A JSON-formatted string terminated with the Record Separator (0x1E).
         * @throws std::invalid_argument If the provided protocol pointer is null.
         */
        inline std::string write_handshake(
            const std::unique_ptr<hub_protocol>& protocol
        ) {
            if (!protocol) {
                throw std::invalid_argument("protocol cannot be null");
            }
            return write_handshake(*protocol);
        }

        /**
         * Parses an incoming server handshake response buffer.
         *
         * Verifies that the buffer contains a complete message terminated by the record separator,
         * validates that the message is a well-formed JSON object, and extracts any remaining payload
         * data along with the parsed handshake message.
         *
         * @param response The raw string buffer received from the server.
         * @return A tuple containing:
         *         - std::string: Any remaining data in the buffer following the record separator.
         *         - signalr::value: The parsed JSON object representation of the handshake response.
         * @throws signalr_exception If the message is incomplete, contains malformed JSON, or is not a JSON object.
         */
        inline std::tuple<std::string, signalr::value> parse_handshake(
            const std::string& response
        ) {
            const auto pos = response.find(JSON_MSG_SEP);
            if (pos == std::string::npos) {
                throw signalr_exception("incomplete message received");
            }

            const auto message = response.substr(0, pos);

            Json::Value root;
            auto reader = getJsonReader();
            std::string errors;

            if (!reader->parse(
                message.c_str(),
                message.c_str() + message.size(),
                &root,
                &errors
            )) {
                throw signalr_exception(errors);
            }

            if (!root.isObject()) {
                throw signalr_exception("handshake response must be a JSON object");
            }

            auto remaining_data = response.substr(pos + 1);
            return std::make_tuple(
                std::move(remaining_data),
                createValue(root)
            );
        }

        /**
         * Strongly-typed overload to parse an incoming handshake response.
         *
         * @param response The raw string buffer received from the server.
         * @return A tuple containing:
         *         - std::string: Any remaining data in the buffer following the record separator.
         *         - handshake_response: The strongly-typed representation of the server's handshake response.
         * @throws signalr_exception If the message is incomplete, contains malformed JSON, or is not a JSON object.
         */
        inline std::tuple<std::string, handshake_response> parse_handshake_message(
            const std::string& response
        ) {
            auto parsed = parse_handshake(response);
            auto remaining_data = std::move(std::get<0>(parsed));
            const auto raw_val = std::move(std::get<1>(parsed));

            handshake_response result;
            result.raw_message = raw_val;

            if (raw_val.is_map()) {
                const auto& map = raw_val.as_map();

                const auto error_it = map.find("error");
                if (error_it != map.end() && error_it->second.is_string()) {
                    result.error = error_it->second.as_string();
                }

                const auto minor_ver_it = map.find("minorVersion");
                if (minor_ver_it != map.end() && minor_ver_it->second.is_double()) {
                    result.minor_version = static_cast<int>(minor_ver_it->second.as_double());
                }
            }

            return std::make_tuple(
                std::move(remaining_data),
                std::move(result)
            );
        }
    }
}
