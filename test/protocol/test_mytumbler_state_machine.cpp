// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <iostream>
#include "test/protocol/mytumbler_state_machine_fixture.h"

#include "consensus/protocol/myba.h"

namespace consensus_spec {
using consensus_spec::ssz_types::AggregateSignature;

#define NOWTIME                                                                                  \
    static_cast<long long unsigned int>(std::chrono::duration_cast<std::chrono::milliseconds>(   \
                                            std::chrono::system_clock::now().time_since_epoch()) \
                                            .count())

class MyTumblerStateMachineTest : public testing::Test, public mytumbler_state_machine_fixture {
  public:
    MyTumblerStateMachineTest() : mytumbler_state_machine_fixture() {}

    void SetUp() override {
        auto new_check_validity =
            [&](Seq seq, const NodeId& id, const std::pair<uint32_t, uint32_t> index_epoch, std::shared_ptr<ABuffer> buffer) {
                engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
                return true;
            };
        engine_->check_validity_ = new_check_validity;

        auto ts = NOWTIME;
        engine_->last_consensus_ts_.store(ts);
        usleep(1000);
    }

    void SetProposers(bool is_proposer) {
        uint32_t start_idx = is_proposer ? 0 : 1;
        std::set<NodeId> proposers;
        for (uint32_t i = 0; i < 4; ++i) {
            proposers.insert(peers_[start_idx + i]);
        }
        std::map<Seq, std::set<NodeId>> next_proposers;
        next_proposers[0] = proposers;
        next_proposers[20] = proposers;
        engine_->next_proposers_ = next_proposers;
        engine_->current_proposers_ = proposers;
        engine_->is_proposer_ = is_proposer;
    }

    PassMessagePtr CreatePassMessageWithIndex(const Seq seq,
                                              const std::vector<uint32_t>& endorsed_peers) {
        std::vector<ssz::ByteVector<32>> endorsed;
        for (auto& peer : endorsed_peers) {
            endorsed.emplace_back(peers_[peer]);
        }
        return CreatePassMessage(seq, std::move(endorsed));
    }

    void SoftPass() {
        asio::error_code ec = asio::error_code();
        engine_->DoOnSoftPassTimeout(ec, false);
    }

    void HardPass() {
        asio::error_code ec = asio::error_code();
        engine_->DoOnHardPassTimeout(ec, false);
    }

    void SkipTimeout() {
        asio::error_code ec = asio::error_code();
        engine_->DoOnSkipSendTimeout(ec, false);
    }

    void ReceivePassMessage(const Seq seq,
                            const std::vector<uint32_t>& senders,
                            const std::vector<uint32_t>& endorsed_peers) {
        auto pass = CreatePassMessageWithIndex(seq, endorsed_peers);
        for (auto& sender : senders) {
            engine_->DoOnRecvPassMessage(peers_[sender], pass);
        }
    }

    void ReceiveValAndDoMyBAComplete(const Seq seq, const NodeId& sender) {
        bytes payload = asBytes("payload" + sender);
        auto v = CreateValMessage(seq, sender, NOWTIME, payload);
        engine_->DoOnRecvValMessage(sender, v, CheckSanityType::RECEIVED_FROM_OTHERS);
        engine_->DoEndorseCallBack(seq, sender, Digest(v->hash.Acquire()));
        engine_->DoMyBACompleteCallBack(seq, sender, Digest(v->hash.Acquire()));
    }

    void ReceiveSkipMessage(const Seq seq, const uint32_t id) {
        auto skip = CreateSkipMessage(seq, peers_[id], Signature());

        skip->signature = MockSignFunc(id, CalculateSkipDigest(skip));
        engine_->DoOnRecvSkipMessage(peers_[id], skip);
    }

    void ReceiveForwardSkipMessage(const Seq seq,
                                   const std::vector<uint32_t>& proposers,
                                   const NodeId& sender) {
        std::vector<ssz_types::Signature> skip_signatures;
        for (auto& id : proposers) {
            auto skip = CreateSkipMessage(seq, peers_[id], Signature());
            skip->signature = MockSignFunc(id, CalculateSkipDigest(skip));

            ssz_types::Signature sig;
            sig.node_id = peers_[id];
            sig.signature = Signature(skip->signature.Acquire());
            skip_signatures.emplace_back(std::move(sig));
        }
        ForwardSkipMessagePtr forward_skip =
            CreateForwardSkipMessage(seq, std::move(skip_signatures));
        engine_->DoOnRecvForwardSkipMessage(sender, forward_skip);
    }
};

// from sync
TEST_F(MyTumblerStateMachineTest, UpdateBySync) {
    SetProposers(false);
    Seq seq = engine_->GetCurrSeq();

    engine_->DoUpdateRawSeq(seq, NOWTIME, true, false);
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);

    engine_->DoUpdateFinishedSequence(seq, {});
    EXPECT_EQ(engine_->last_finished_seq_, seq);

    engine_->DoUpdateStableSeq(seq, {});
    EXPECT_EQ(engine_->last_stable_seq_, seq);
}

TEST_F(MyTumblerStateMachineTest, Passed_UpdateBySync) {
    SetProposers(false);
    Seq seq = engine_->GetCurrSeq();

    HardPass();

    engine_->DoUpdateRawSeq(seq, NOWTIME, true, false);
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);

    engine_->DoUpdateFinishedSequence(seq, {});
    EXPECT_EQ(engine_->last_finished_seq_, seq);

    engine_->DoUpdateStableSeq(seq, {});
    EXPECT_EQ(engine_->last_stable_seq_, seq);
}

TEST_F(MyTumblerStateMachineTest, UpdateBySync_Invalid) {
    SetProposers(false);
    Seq seq = engine_->GetCurrSeq();

    engine_->DoUpdateRawSeq(seq + 9, NOWTIME, true, false);
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 10);

    engine_->DoUpdateFinishedSequence(seq + 9, {});
    EXPECT_EQ(engine_->last_finished_seq_, seq + 9);

    EXPECT_DEATH(engine_->DoUpdateStableSeq(seq + 9, {}), "");
}

// not proposer
TEST_F(MyTumblerStateMachineTest, NotProposer_TryPropose) {
    SetProposers(false);

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    engine_->DoPropose(proposal, false, NOWTIME, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_FAILED_NOT_PROPOSER);
}

// To N2

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromHardPass) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    // no one propose, trigger by others' hard pass
    ReceivePassMessage(seq, {2, 3, 4, 5, 6}, {});
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromPassAndAllComplete) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2, 3, 4});
    EXPECT_EQ(engine_->passed_.size(), 5);
    // quorom passed, but not all complete, waiting pending val complte
    EXPECT_EQ(engine_->pending_[seq].size(), 4);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 3);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceiveValAndDoMyBAComplete(seq, peers_[4]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromPassAndAllComplete_WaitingVal) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2, 3, 4});
    EXPECT_EQ(engine_->passed_.size(), 5);
    // quorom passed, but not all complete, waiting pending val complte
    EXPECT_EQ(engine_->pending_[seq].size(), 4);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 3);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    // ba success, waiting val
    bytes payload = asBytes("payload" + peers_[4]);
    auto v = CreateValMessage(seq, peers_[4], NOWTIME, payload);
    engine_->DoEndorseCallBack(seq, peers_[4], Digest(v->hash.Acquire()));
    engine_->DoMyBACompleteCallBack(seq, peers_[4], Digest(v->hash.Acquire()));
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    engine_->DoOnRequestProposalTimeout(asio::error_code(), false);

    auto response = std::make_shared<ssz_types::ResponseProposalMessage>();
    std::vector<ValMessagePtr> vals;
    vals.emplace_back(v);
    response->vals = std::move(vals);
    engine_->DoOnRecvResponseProposalMessage(peers_[4], response);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromPassAndSkipMixComplete) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveSkipMessage(seq, 3);
    EXPECT_EQ(engine_->skipped_.size(), 1);

    ReceiveSkipMessage(seq, 4);
    EXPECT_EQ(engine_->skipped_.size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromPassAndSkipMixComplete_ForwardSkip) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveForwardSkipMessage(seq, {3, 4}, peers_[3]);
    EXPECT_EQ(engine_->skipped_.size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromPassAndHardPass) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 6);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_QuoromPassAndSoftPass) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    // at least 1 ba success
    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 6);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// To N3

TEST_F(MyTumblerStateMachineTest, NotProposer_CompleteByQuoromHardPass_Self) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    // no one propose, trigger by hard pass
    HardPass();
    EXPECT_TRUE(engine_->passed_.count(peers_[0]));
    ReceivePassMessage(seq, {2, 3, 4, 5}, {});
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_AllCompleteAndQuoromPass) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_TRUE(engine_->ba_complete_.count(peers_[1]));

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_TRUE(engine_->ba_complete_.count(peers_[2]));

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);
    EXPECT_TRUE(engine_->ba_complete_.count(peers_[3]));

    ReceiveValAndDoMyBAComplete(seq, peers_[4]);
    EXPECT_TRUE(engine_->ba_complete_.count(peers_[4]));

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2, 3, 4});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_SkipMixCompleteAndQuoromPass) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceiveSkipMessage(seq, 3);
    EXPECT_EQ(engine_->skipped_.size(), 1);

    ReceiveSkipMessage(seq, 4);
    EXPECT_EQ(engine_->skipped_.size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_SkipMixCompleteAndQuoromPass_ForwardSkip) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceiveForwardSkipMessage(seq, {3, 4}, peers_[3]);
    EXPECT_EQ(engine_->skipped_.size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_SoftPassAndQuoromPass) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 3);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 4);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    ReceivePassMessage(seq, {3}, {1, 2});
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_HardPassAndCompleteAndQuoromPass) {
    SetProposers(false);
    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 3);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 4);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);

    ReceivePassMessage(seq, {3}, {1, 2});
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_AllCompleteAndQuoromPass_WaitingVal) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    // ba success, waiting val
    bytes payload = asBytes("payload" + peers_[4]);
    auto v = CreateValMessage(seq, peers_[4], NOWTIME, payload);
    engine_->DoEndorseCallBack(seq, peers_[4], Digest(v->hash.Acquire()));
    engine_->DoMyBACompleteCallBack(seq, peers_[4], Digest(v->hash.Acquire()));
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2, 3, 4});
    EXPECT_EQ(engine_->passed_.size(), 5);
    // quorom passed, but not all complete, waiting pending val complte

    engine_->DoOnRequestProposalTimeout(asio::error_code(), false);

    auto response = std::make_shared<ssz_types::ResponseProposalMessage>();
    std::vector<ValMessagePtr> vals;
    vals.emplace_back(v);
    response->vals = std::move(vals);
    engine_->DoOnRecvResponseProposalMessage(peers_[4], response);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, NotProposer_RemoveInvalidPending) {
    SetProposers(false);

    Seq seq = engine_->GetCurrSeq();

    bytes payload3 = asBytes("payload3");
    auto v3 = CreateValMessage(seq, peers_[3], NOWTIME, payload3);
    engine_->DoOnRecvValMessage(peers_[3], v3, CheckSanityType::RECEIVED_FROM_OTHERS);
    engine_->DoEndorseCallBack(seq, peers_[3], Digest(v3->hash.Acquire()));
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceivePassMessage(seq, {1, 2, 5}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 3);
    EXPECT_EQ(engine_->pending_[seq].size(), 3);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);

    ReceiveSkipMessage(seq, 3);
    EXPECT_EQ(engine_->skipped_.size(), 1);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceiveSkipMessage(seq, 4);
    EXPECT_EQ(engine_->skipped_.size(), 2);

    EXPECT_EQ(engine_->passed_.size(), 4);
    ReceivePassMessage(seq, {3, 4}, {1, 2});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// proposer

TEST_F(MyTumblerStateMachineTest, Proposed_UpdateBySync) {
    SetProposers(true);
    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    engine_->DoPropose(proposal, false, NOWTIME, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    engine_->DoUpdateRawSeq(seq, NOWTIME, true, false);
    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);

    engine_->DoUpdateFinishedSequence(seq, {});
    EXPECT_EQ(engine_->last_finished_seq_, seq);

    engine_->DoUpdateStableSeq(seq, {});
    EXPECT_EQ(engine_->last_stable_seq_, seq);
}

TEST_F(MyTumblerStateMachineTest, Proposer_ProposeAfterSkipped) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    SkipTimeout();
    EXPECT_EQ(engine_->skipped_.size(), 1);
    // No Pass yet, so propose_seq_ stays at seq and is_proposed_ is true -> propose should fail.
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), seq);
    EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    engine_->DoPropose(proposal, false, NOWTIME, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS);
    EXPECT_EQ(state.second, 0);
}

TEST_F(MyTumblerStateMachineTest, Proposer_ProposeAfterSkippedThenPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    SkipTimeout();
    EXPECT_EQ(engine_->skipped_.size(), 1);
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), seq);

    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 1);
    // After Pass, propose_state_ advances to seq+1 (we skipped so Pass() advances).
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), seq + 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    engine_->DoPropose(proposal, false, NOWTIME, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_ProposeAfterProposed) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal1 = asBytes("proposal1");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom1;
    engine_->DoPropose(proposal1, false, NOWTIME, &prom1);

    auto future1 = prom1.get_future();
    auto state1 = future1.get();
    EXPECT_EQ(state1.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state1.second, seq);

    bytes proposal2 = asBytes("proposal2");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom2;
    engine_->DoPropose(proposal2, false, NOWTIME, &prom2);

    // failed
    auto future2 = prom2.get_future();
    auto state2 = future2.get();
    EXPECT_EQ(state2.first, MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS);
    EXPECT_EQ(state2.second, 0);
}

TEST_F(MyTumblerStateMachineTest, Proposer_ProposeAfterPassed) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    engine_->DoPropose(proposal, false, NOWTIME, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_SkipAfterProposed) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal1 = asBytes("proposal1");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom1;
    engine_->DoPropose(proposal1, false, NOWTIME, &prom1);

    auto future1 = prom1.get_future();
    auto state1 = future1.get();
    EXPECT_EQ(state1.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state1.second, seq);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    SkipTimeout();
    EXPECT_EQ(engine_->skipped_.size(), 0);
}

TEST_F(MyTumblerStateMachineTest, Proposer_SkipAfterSkipped) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    SkipTimeout();
    EXPECT_EQ(engine_->skipped_.size(), 1);

    ReceiveSkipMessage(seq, 0);
    EXPECT_EQ(engine_->skipped_.size(), 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_SkipAfterPassed) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    SkipTimeout();
    EXPECT_EQ(engine_->skipped_.size(), 0);
}

// M2

TEST_F(MyTumblerStateMachineTest, Proposer_QuoromPassed_EmptyConsensus) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_QuoromPassed_HardPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {2, 3, 4, 5, 6}, {1});
    EXPECT_EQ(engine_->passed_.size(), 5);
    HardPass();
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_QuoromPassed_SoftPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2});

    EXPECT_EQ(engine_->passed_.size(), 5);
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 6);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// M1->S3
TEST_F(MyTumblerStateMachineTest, Proposer_SkipByHardPass_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 1);
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2});
    EXPECT_EQ(engine_->passed_.size(), 5);

    ReceiveValAndDoMyBAComplete(seq, peers_[2]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_SkipByHardPass_QuoromPass_EmptyConsensus) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// M1->S1->S2

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_QuoromPass_SoftPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    SkipTimeout();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2, 3});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 6);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_QuoromPass_AllComplete) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    SkipTimeout();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2, 3});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    EXPECT_EQ(engine_->passed_.size(), 5);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_QuoromPass_HardPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    SkipTimeout();

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2, 3});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 2);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    EXPECT_EQ(engine_->passed_.size(), 5);
    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 6);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// M1->S1->S3
TEST_F(MyTumblerStateMachineTest, Proposer_Skip_HardPass_QuoromPass_EmptyConsensus) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    SkipTimeout();

    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_HardPass_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    SkipTimeout();
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2, 3});
    EXPECT_EQ(engine_->passed_.size(), 5);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_SoftPass_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    SkipTimeout();
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2, 3});
    EXPECT_EQ(engine_->passed_.size(), 5);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_AllSuccess_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    SkipTimeout();
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    ReceiveValAndDoMyBAComplete(seq, peers_[3]);
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1, 2, 3});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_OnlyOneProposal_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    SkipTimeout();
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveSkipMessage(seq, 2);
    ReceiveSkipMessage(seq, 3);
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {1});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Skip_OnlyOneProposal_QuoromPass_ForwardSkip) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    SkipTimeout();
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveForwardSkipMessage(seq, {2, 3}, peers_[1]);
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 4, 5, 6}, {1});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// proposer, proposed, i.e. P*

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_SoftPass_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);

    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    SoftPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {0, 1, 2, 3});
    EXPECT_EQ(engine_->passed_.size(), 5);
    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_AllComplete_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);

    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    ReceiveValAndDoMyBAComplete(seq, peers_[3]);
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {0, 1, 2, 3});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_OthersSkipped_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);

    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    ReceiveSkipMessage(seq, 1);
    ReceiveSkipMessage(seq, 2);
    ReceiveSkipMessage(seq, 3);
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 3, 4}, {0});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_OthersSkipped_QuoromPass_ForwardSkip) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);

    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    ReceiveSkipMessage(seq, 1);
    ReceiveForwardSkipMessage(seq, {2, 3}, peers_[0]);
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 5, 6}, {0});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_HardPass_QuoromPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 1);

    ReceivePassMessage(seq, {1, 2, 5, 6}, {});
    EXPECT_EQ(engine_->passed_.size(), 5);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    // 2f+1 pass, remove pending
    ReceivePassMessage(seq, {3}, {});

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_QuoromPass_Complete) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ReceivePassMessage(seq, {1, 2, 4, 5, 6}, {0, 1});

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);

    EXPECT_EQ(engine_->passed_.size(), 5);

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);
    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_QuoromPass_CompleteAndSkipped) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ReceiveSkipMessage(seq, 1);
    ReceiveSkipMessage(seq, 2);
    ReceiveSkipMessage(seq, 3);

    ReceivePassMessage(seq, {1, 2, 3, 5, 6}, {0});
    EXPECT_EQ(engine_->passed_.size(), 5);

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);
    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_QuoromPass_SoftPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ReceivePassMessage(seq, {1, 2, 4, 5, 6}, {0});

    EXPECT_EQ(engine_->passed_.size(), 5);

    SoftPass();

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);
    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_QuoromPass_HardPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ReceivePassMessage(seq, {1, 2, 4, 5, 6}, {0});

    EXPECT_EQ(engine_->passed_.size(), 5);

    HardPass();

    ValMessagePtr val =
        CreateValMessage(seq, peers_[0], timestamp, std::move(proposal), engine_->epoch_number_);
    EXPECT_TRUE(engine_->pending_[seq].count(peers_[0]));
    engine_->DoMyBACompleteCallBack(seq, peers_[0], Digest(val->hash.Acquire()));

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_Proposed_QuoromPass_Removed) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_SUCCESS);
    EXPECT_EQ(state.second, seq);

    ReceivePassMessage(seq, {1, 2, 4, 5, 6}, {1, 2, 3});

    EXPECT_EQ(engine_->passed_.size(), 5);

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);
    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

TEST_F(MyTumblerStateMachineTest, Proposer_QuoromPass_Proposed) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveValAndDoMyBAComplete(seq, peers_[1]);
    ReceiveValAndDoMyBAComplete(seq, peers_[2]);

    ReceivePassMessage(seq, {1, 2, 3, 4, 5}, {1, 2, 3});

    EXPECT_EQ(engine_->passed_.size(), 5);

    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS);
    EXPECT_EQ(state.second, 0);

    ReceiveValAndDoMyBAComplete(seq, peers_[3]);

    EXPECT_EQ(engine_->GetCurrSeq(), seq + 1);
}

// window full
TEST_F(MyTumblerStateMachineTest, Proposer_WindowFull) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    engine_->DoUpdateRawSeq(seq + engine_->cfg_.consensus_window_, NOWTIME, true, false);

    // try to propose
    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL);

    // try to pass
    HardPass();
    EXPECT_EQ(engine_->passed_.size(), 0);
}

// epoch end

TEST_F(MyTumblerStateMachineTest, Proposer_EpochEnd) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    engine_->DoUpdateRawSeq(seq, NOWTIME, true, true);

    // try to propose
    bytes proposal = asBytes("proposal");
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    auto timestamp = NOWTIME;
    engine_->DoPropose(proposal, false, timestamp, &prom);

    auto future = prom.get_future();
    auto state = future.get();
    EXPECT_EQ(state.first, MyTumblerProposeState::PROPOSE_FAILED_EPOCH_END);

    ReceiveSkipMessage(seq, 1);
    EXPECT_EQ(engine_->skipped_.size(), 0);

    ReceivePassMessage(seq, {1}, {});
    EXPECT_EQ(engine_->passed_.size(), 0);
}

// invalid case

TEST_F(MyTumblerStateMachineTest, ReceiveValAfterSkip) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveSkipMessage(seq, 1);
    bytes payload = asBytes("payload" + peers_[1]);
    auto v = CreateValMessage(seq, peers_[1], NOWTIME, payload);
    engine_->DoOnRecvValMessage(peers_[1], v, CheckSanityType::RECEIVED_FROM_OTHERS);

    // EXPECT_EQ(engine_->pending_[seq].size(), 0);
}

TEST_F(MyTumblerStateMachineTest, ReceiveValAfterForwardSkip) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceiveForwardSkipMessage(seq, {1}, peers_[2]);
    bytes payload = asBytes("payload" + peers_[1]);
    auto v = CreateValMessage(seq, peers_[1], NOWTIME, payload);
    engine_->DoOnRecvValMessage(peers_[1], v, CheckSanityType::RECEIVED_FROM_OTHERS);

    // EXPECT_EQ(engine_->pending_[seq].size(), 0);
}

TEST_F(MyTumblerStateMachineTest, ReceiveSkipAfterVal) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes payload = asBytes("payload" + peers_[1]);
    auto v = CreateValMessage(seq, peers_[1], NOWTIME, payload);
    engine_->DoOnRecvValMessage(peers_[1], v, CheckSanityType::RECEIVED_FROM_OTHERS);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);

    ReceiveSkipMessage(seq, 1);
    EXPECT_EQ(engine_->skipped_.size(), 1);
}

TEST_F(MyTumblerStateMachineTest, ReceiveValAfterPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    ReceivePassMessage(seq, {1}, {});

    bytes payload = asBytes("payload" + peers_[1]);
    auto v = CreateValMessage(seq, peers_[1], NOWTIME, payload);
    engine_->DoOnRecvValMessage(peers_[1], v, CheckSanityType::RECEIVED_FROM_OTHERS);
    // EXPECT_EQ(engine_->pending_[seq].size(), 0);
}

TEST_F(MyTumblerStateMachineTest, ReceiveValAfterSelfPass) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    HardPass();

    bytes payload = asBytes("payload" + peers_[1]);
    auto v = CreateValMessage(seq, peers_[1], NOWTIME, payload);
    engine_->DoOnRecvValMessage(peers_[1], v, CheckSanityType::RECEIVED_FROM_OTHERS);
    EXPECT_TRUE(engine_->already_vote_zero_.count(peers_[1]));
}

TEST_F(MyTumblerStateMachineTest, ReceiveVal2AfterVal1) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes payload1 = asBytes("payload1");
    auto v1 = CreateValMessage(seq, peers_[1], NOWTIME, payload1);
    engine_->DoOnRecvValMessage(peers_[1], v1, CheckSanityType::RECEIVED_FROM_OTHERS);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);
    EXPECT_TRUE(engine_->val_[seq][peers_[1]].count(Digest(v1->hash.Acquire())));

    bytes payload2 = asBytes("payload2");
    auto v2 = CreateValMessage(seq, peers_[1], NOWTIME, payload2);
    engine_->DoOnRecvValMessage(peers_[1], v2, CheckSanityType::RECEIVED_FROM_OTHERS);
    EXPECT_EQ(engine_->pending_[seq].size(), 1);
    EXPECT_TRUE(engine_->val_[seq][peers_[1]].count(Digest(v1->hash.Acquire())));
}

TEST_F(MyTumblerStateMachineTest, ReceiveValFromNotProposer) {
    SetProposers(true);

    Seq seq = engine_->GetCurrSeq();

    bytes payload = asBytes("payload");
    auto v = CreateValMessage(seq, peers_[6], NOWTIME, payload);
    engine_->DoOnRecvValMessage(peers_[6], v, CheckSanityType::RECEIVED_FROM_OTHERS);
    EXPECT_EQ(engine_->pending_[seq].size(), 0);
    EXPECT_FALSE(engine_->val_[seq][peers_[1]].count(Digest(v->hash.Acquire())));
}

}  // namespace consensus_spec