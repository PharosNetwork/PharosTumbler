// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <chrono>

namespace consensus_spec {

using SteadyTimePoint = std::chrono::steady_clock::time_point;

namespace time_utils {
SteadyTimePoint GetSteadyTimePoint();
double GetDuration(const SteadyTimePoint& start);
double GetDuration(const SteadyTimePoint& start, const SteadyTimePoint& end);
uint64_t GetCurrentTimestamp();
}  // namespace time_utils
}  // namespace consensus_spec
