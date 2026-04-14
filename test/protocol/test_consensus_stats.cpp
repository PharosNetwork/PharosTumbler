// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "mytumbler_engine_fixture.h"

namespace consensus_spec {

TEST(ConsensusStatsTest, NotInitializedBeforeConfigure) {
    MyTumblerEngineBase engine;
    auto r = engine.GetConsensusStats();
    EXPECT_EQ(r.first, ConsensusStatsError::kNotInitialized);
    EXPECT_FALSE(r.second.has_value());
}

TEST(ConsensusStatsTest, StoppedAfterConfigureBeforeStart) {
    mytumbler_engine_fixture f;
    auto r = f.engine_->GetConsensusStats();
    EXPECT_EQ(r.first, ConsensusStatsError::kEngineStopped);
    EXPECT_FALSE(r.second.has_value());
}

TEST(ConsensusStatsTest, OkAfterStartAndBaselineFromConfigure) {
    mytumbler_engine_fixture f;
    f.engine_->Start();
    auto r = f.engine_->GetConsensusStats();
    ASSERT_EQ(r.first, ConsensusStatsError::kOk);
    ASSERT_TRUE(r.second.has_value());
    const auto& s = *r.second;
    EXPECT_EQ(s.consensus_version, CONSENSUS_VERSION_PIPELINE_PROPOSE);
    EXPECT_EQ(s.engine_stats_block_count, 0u);
    EXPECT_EQ(s.latest_block_number, 0u);
    EXPECT_EQ(s.validator_number, f.engine_->n_);
    EXPECT_EQ(s.self_consensus_completed_by_sync_count, 0u);
    f.engine_->Stop();
    auto stopped = f.engine_->GetConsensusStats();
    EXPECT_EQ(stopped.first, ConsensusStatsError::kEngineStopped);
}

}  // namespace consensus_spec
