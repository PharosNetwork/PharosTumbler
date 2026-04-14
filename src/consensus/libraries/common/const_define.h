// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdlib>
#include <string>

namespace consensus_spec {

const size_t MYTUMBLER_THREADS_MAX = 50;
const size_t MYTUMBLER_BATCH_SIZE_DEFAULT = 256;

const std::string ZERO_32_BYTES = "00000000000000000000000000000000";

enum ConsensusVersion : uint16_t {
    CONSENSUS_VERSION_PIPELINE_PROPOSE = 7,
    CONSENSUS_VERSION_NO_TIMESTAMP_CHECK = 9,
    CONSENSUS_VERSION_DECIDE_WITH_CERT = 10,
};

// Minimum supported consensus version (versions < MIN are no longer supported).
constexpr uint16_t CONSENSUS_VERSION_MIN = CONSENSUS_VERSION_PIPELINE_PROPOSE;

}  // namespace consensus_spec
