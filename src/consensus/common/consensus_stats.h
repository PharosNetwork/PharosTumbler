// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <utility>

#include "consensus/libraries/common/type_define.h"

namespace consensus_spec {

struct PeerConsensusStats {
    uint64_t inactive_count{0};
};

struct ConsensusStats {
    uint32_t stats_schema_revision{1};
    uint32_t consensus_version{0};
    uint64_t engine_stats_block_count{0};
    Seq latest_block_number{0};
    uint32_t validator_number{0};
    uint64_t self_consensus_completed_by_sync_count{0};
    uint64_t self_proposal_success_count{0};
    uint64_t self_proposal_rejected_count{0};
    uint64_t self_skip_count{0};
    std::map<NodeId, PeerConsensusStats> peer_stats;
};

enum class ConsensusStatsError {
    kOk = 0,
    kNotInitialized,
    kEngineStopped,
    kUnavailable,
};

}  // namespace consensus_spec
