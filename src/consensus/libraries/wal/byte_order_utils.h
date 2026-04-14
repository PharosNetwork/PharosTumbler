// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <string_view>
#include <cstdint>

namespace consensus_spec {
namespace wal {

namespace byte_order_utils {

// Write functions - big endian
void Writeu32_be(uint32_t val, std::string& data);
void Writeu64_be(uint64_t val, std::string& data);

// Read functions - big endian
uint32_t Readu32_be(const std::string_view& data);
uint64_t Readu64_be(const std::string_view& data);

}  // namespace byte_order_utils
}  // namespace wal
}  // namespace consensus_spec
