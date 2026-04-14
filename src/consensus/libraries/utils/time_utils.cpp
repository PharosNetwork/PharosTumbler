// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/utils/time_utils.h"

#include <cstdlib>

namespace consensus_spec {
namespace time_utils {
SteadyTimePoint GetSteadyTimePoint() {
    return std::chrono::steady_clock::now();
}

double GetDuration(const SteadyTimePoint& start, const SteadyTimePoint& end) {
    return std::abs(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count())
           / 1e9;
}

double GetDuration(const SteadyTimePoint& start) {
    auto end = std::chrono::steady_clock::now();
    return std::abs(std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count())
           / 1e9;
}

uint64_t GetCurrentTimestamp() {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    uint64_t millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    return millis;
}

}  // namespace time_utils
}  // namespace consensus_spec
