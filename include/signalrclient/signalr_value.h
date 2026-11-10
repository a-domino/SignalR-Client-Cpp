// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

#pragma once

#include "_exports.h"
#include <string>
#include <vector>
#include <map>
#include <cstddef>

#ifdef USE_MSGPACK
#include <msgpack/v3/object_fwd_decl.hpp>
#endif

namespace signalr
{
    /**
     * An enum defining the types a signalr::value may be.
     */
    enum class value_type
    {
        map,
        array,
        string,
        float64,
        null,
        boolean,
        binary,
        ext
    };

    /**
     * Represents a MessagePack Extension (EXT) object, consisting of an extension type code
     * and a payload of binary data.
     */
    struct messagepack_ext {
        int8_t type_code{0};
        std::vector<uint8_t> data;

        /**
         * Create an EXT object by moving the given binary payload.
         *
         * @param type_code Application- or format-defined extension type identifier (-128 to 127).
         * @param data Binary payload to take ownership of.
         */
        messagepack_ext(const int8_t type_code, std::vector<uint8_t>&& data)
            : type_code(type_code), data(std::move(data)) {}

        messagepack_ext(const messagepack_ext& other)
            : type_code(other.type_code), data(std::vector<uint8_t>(other.data)) {}

        messagepack_ext(messagepack_ext&& other) noexcept : type_code(other.type_code), data(std::move(other.data)) {}

        ~messagepack_ext() = default;
    };

    /**
     * Represents a value to be provided to a SignalR method as a parameter, or returned as a return value.
     */
    class value
    {
    public:
        /**
         * Create an object representing a value_type::null value.
         */
        SIGNALRCLIENT_API value();

        /**
         * Create an object representing a value_type::null value.
         */
        SIGNALRCLIENT_API value(std::nullptr_t);

        /**
         * Create an object representing a default value for the given value_type.
         */
        SIGNALRCLIENT_API value(value_type t);

        /**
         * Create an object representing a value_type::boolean with the given bool value.
         */
        SIGNALRCLIENT_API value(bool val);

        /**
         * Create an object representing a value_type::float64 with the given double value.
         */
        SIGNALRCLIENT_API value(double val);

        /**
         * Create an object representing a value_type::string with the given string value.
         */
        SIGNALRCLIENT_API value(const std::string& val);

        /**
         * Create an object representing a value_type::string with the given string value.
         */
        SIGNALRCLIENT_API value(std::string&& val);

        /**
         * Create an object representing a value_type::string with the given string value.
         */
        SIGNALRCLIENT_API value(const char* val);

        /**
         * Create an object representing a value_type::string with the given string value.
         */
        SIGNALRCLIENT_API value(const char* val, size_t length);

        /**
         * Create an object representing a value_type::array with the given vector of value's.
         */
        SIGNALRCLIENT_API value(const std::vector<value>& val);

        /**
         * Create an object representing a value_type::array with the given vector of value's.
         */
        SIGNALRCLIENT_API value(std::vector<value>&& val);

        /**
         * Create an object representing a value_type::map with the given map of string-value's.
         */
        SIGNALRCLIENT_API value(const std::map<std::string, value>& map);

        /**
         * Create an object representing a value_type::map with the given map of string-value's.
         */
        SIGNALRCLIENT_API value(std::map<std::string, value>&& map);

        /**
         * Create an object representing a value_type::binary with the given array of byte's.
         */
        SIGNALRCLIENT_API value(const std::vector<uint8_t>& bin);

        /**
         * Create an object representing a value_type::binary with the given array of byte's.
         */
        SIGNALRCLIENT_API value(std::vector<uint8_t>&& bin);

#ifdef USE_MSGPACK

        /**
         * Creates an object representing a value_type::ext from the given MessagePack ext.
         */
        SIGNALRCLIENT_API value(const msgpack::object_ext& ext_obj);

#endif // USE_MSGPACK

        /**
         * Creates an object representing a value_type::ext from the given type code and data.
         */
        SIGNALRCLIENT_API value(int8_t type_code, const std::vector<uint8_t>& data);

        /**
         * Creates an object representing a value_type::ext with the given type code and data.
         */
        SIGNALRCLIENT_API value(int8_t type_code, std::vector<uint8_t>&& data);

        /**
         * Copies an existing value.
         */
        SIGNALRCLIENT_API value(const value& rhs);

        /**
         * Moves an existing value.
         */
        SIGNALRCLIENT_API value(value&& rhs) noexcept;

        /**
         * Cleans up the resources associated with the value.
         */
        SIGNALRCLIENT_API ~value();

        /**
         * Copies an existing value.
         */
        SIGNALRCLIENT_API value& operator=(const value& rhs);

        /**
         * Moves an existing value.
         */
        SIGNALRCLIENT_API value& operator=(value&& rhs) noexcept;

        /**
         * True if the object stored is a Key-Value pair.
         */
        SIGNALRCLIENT_API bool is_map() const;

        /**
         * True if the object stored is a double.
         */
        SIGNALRCLIENT_API bool is_double() const;

        /**
         * True if the object stored is a string.
         */
        SIGNALRCLIENT_API bool is_string() const;

        /**
         * True if the object stored is empty.
         */
        SIGNALRCLIENT_API bool is_null() const;

        /**
         * True if the object stored is an array of signalr::value's.
         */
        SIGNALRCLIENT_API bool is_array() const;

        /**
         * True if the object stored is a bool.
         */
        SIGNALRCLIENT_API bool is_bool() const;

        /**
         * True if the object stored is a binary blob.
         */
        SIGNALRCLIENT_API bool is_binary() const;

        /**
         * True if the object stored is a representation of a MessagePack EXT object.
         */
        SIGNALRCLIENT_API bool is_ext() const;

        /**
         * Returns the stored object as a double. This will throw if the underlying object is not a signalr::type::float64.
         */
        SIGNALRCLIENT_API double as_double() const;

        /**
         * Returns the stored object as a bool. This will throw if the underlying object is not a signalr::type::boolean.
         */
        SIGNALRCLIENT_API bool as_bool() const;

        /**
         * Returns the stored object as a string. This will throw if the underlying object is not a signalr::type::string.
         */
        SIGNALRCLIENT_API const std::string& as_string() const;

        /**
         * Returns the stored object as an array of signalr::value's. This will throw if the underlying object is not a signalr::type::array.
         */
        SIGNALRCLIENT_API const std::vector<value>& as_array() const;

        /**
         * Returns the stored object as a map of property name to signalr::value. This will throw if the underlying object is not a signalr::type::map.
         */
        SIGNALRCLIENT_API const std::map<std::string, value>& as_map() const;

        /**
         * Returns the stored object as an array of bytes. This will throw if the underlying object is not a signalr::type::binary.
         */
        SIGNALRCLIENT_API const std::vector<uint8_t>& as_binary() const;

        /**
         * Returns the stored object as a representation of the MessagePack's EXT object. This will throw if the
         * underlying object is not a signalr::type::ext.
         */
        SIGNALRCLIENT_API const messagepack_ext& as_ext() const;

        /**
         * Returns the signalr::type that represents the stored object.
         */
        SIGNALRCLIENT_API value_type type() const;

    private:
        value_type mType;

        union storage
        {
#pragma warning (push)
#pragma warning (disable: 4582)
            bool boolean{};
            std::string string;
            std::vector<value> array;
            double number;
            std::map<std::string, value> map;
            std::vector<uint8_t> binary;
            messagepack_ext ext;

            // constructor of types in union are not implicitly called
            // this is expected as we only construct a single type in the union once we know
            // what that type is when constructing the signalr_value type.

            storage() {}
#pragma warning (pop)

            storage(const storage&) = delete;
            storage& operator=(const storage&) = delete;

            // destructor is not implicitly called
#pragma warning (push)
#pragma warning (disable: 4583)
            ~storage() {}
#pragma warning (pop)
        };

        storage mStorage;

        void destruct_internals();
    };

    std::ostream& operator<<(std::ostream& os, const signalr::value& value);
}
