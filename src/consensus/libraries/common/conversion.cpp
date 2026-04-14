// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/common/conversion.h"

namespace consensus_spec {

std::string toHex(const uint8_t* data, int length) {
    std::string hex;
    hex.reserve(length << 1);
    static const char* hexdigits = "0123456789abcdef";
    for (int i = 0; i < length; ++i) {
        uint8_t cur = data[i];
        auto h = (cur >> 4) & 0x0f;
        auto l = cur & 0x0f;
        if (h > 15 || l > 15) {
            return std::string();
        }
        hex.push_back(hexdigits[h]);
        hex.push_back(hexdigits[l]);
    }

    return hex;
}

}  // namespace consensus_spec
