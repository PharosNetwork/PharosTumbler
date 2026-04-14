// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/wal/byte_order_utils.h"

namespace consensus_spec {
namespace wal {
namespace byte_order_utils {

void Writeu32_be(uint32_t val, std::string& data) {
    data.push_back(static_cast<char>((val >> 24) & 0xFF));
    data.push_back(static_cast<char>((val >> 16) & 0xFF));
    data.push_back(static_cast<char>((val >> 8) & 0xFF));
    data.push_back(static_cast<char>(val & 0xFF));
}

void Writeu64_be(uint64_t val, std::string& data) {
    data.push_back(static_cast<char>((val >> 56) & 0xFF));
    data.push_back(static_cast<char>((val >> 48) & 0xFF));
    data.push_back(static_cast<char>((val >> 40) & 0xFF));
    data.push_back(static_cast<char>((val >> 32) & 0xFF));
    data.push_back(static_cast<char>((val >> 24) & 0xFF));
    data.push_back(static_cast<char>((val >> 16) & 0xFF));
    data.push_back(static_cast<char>((val >> 8) & 0xFF));
    data.push_back(static_cast<char>(val & 0xFF));
}

uint32_t Readu32_be(const std::string_view& data) {
    if (data.size() < 4) {
        return 0;
    }
    return (static_cast<uint32_t>(static_cast<uint8_t>(data[0])) << 24)
           | (static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 16)
           | (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8)
           | static_cast<uint32_t>(static_cast<uint8_t>(data[3]));
}

uint64_t Readu64_be(const std::string_view& data) {
    if (data.size() < 8) {
        return 0;
    }
    return (static_cast<uint64_t>(static_cast<uint8_t>(data[0])) << 56)
           | (static_cast<uint64_t>(static_cast<uint8_t>(data[1])) << 48)
           | (static_cast<uint64_t>(static_cast<uint8_t>(data[2])) << 40)
           | (static_cast<uint64_t>(static_cast<uint8_t>(data[3])) << 32)
           | (static_cast<uint64_t>(static_cast<uint8_t>(data[4])) << 24)
           | (static_cast<uint64_t>(static_cast<uint8_t>(data[5])) << 16)
           | (static_cast<uint64_t>(static_cast<uint8_t>(data[6])) << 8)
           | static_cast<uint64_t>(static_cast<uint8_t>(data[7]));
}

}  // namespace byte_order_utils
}  // namespace wal
}  // namespace consensus_spec
