// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string.h>
#include <array>
#include <sstream>
#include <string_view>
#include <vector>
#include <cstdint>

namespace consensus_spec {
using byte = uint8_t;
using bytes = std::vector<byte>;

std::string toHex(const uint8_t* data, int length);

inline std::string toHex(const char* data, int length) {
    return toHex(reinterpret_cast<const uint8_t*>(data), length);
}

inline std::string toHex(const std::string& data) {
    return toHex(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

inline std::string toHex(const std::string_view& data) {
    return toHex(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

inline std::string toHex(const std::vector<uint8_t>& data) {
    return toHex(data.data(), data.size());
}

inline bytes toBytes(const std::string& data) {
    bytes ret(data.size());
    memcpy(&ret[0], data.data(), data.size());
    return ret;
}

}  // namespace consensus_spec
