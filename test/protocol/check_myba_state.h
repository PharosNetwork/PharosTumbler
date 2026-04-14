// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/protocol/myba.h"
#include "test/protocol/mock_reliable_channel.h"

namespace consensus_spec {

struct MyBAState {
    Seq seq_ = 1;
    uint32_t proposer_index_;
    NodeId proposer_node_;
    Digest proposal_hash_;
    Round round_ = 0;

    uint16_t spec_version_ = 10;
    uint64_t epoch_number_ = 0;

    MyBAPtr myba_no_aggrbval_;
    std::map<Seq, std::map<NodeId, Digest>> endorsed_;
    std::map<Seq, std::map<NodeId, Digest>> myba_completed_;
    std::shared_ptr<MockReliableChannel> rc_;

    bool CheckIsNormalPath() {
        auto proposal = ProposalKey(seq_, proposer_node_);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        return msg_pool->IsNormalPath();
    }

    bool CheckIsDecided() {
        auto proposal = ProposalKey(seq_, proposer_node_);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        return msg_pool->IsDecided();
    }

    bool CheckRound(Round round) {
        // mock
        auto proposal = ProposalKey(seq_, proposer_node_);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        return msg_pool->GetCurrentRound() == round;
    }

    bool IsBvalMessage(const MessagePtr& msg, const Digest& hash) {
        if (msg->Index() != ConsensusMessageType::BvalMessage) {
            return false;
        }
        auto bval_msg = msg->BvalData();
        if (bval_msg == nullptr) {
            return false;
        }
        return bval_msg->seq == seq_ && bval_msg->hash.Acquire() == hash;
    }

    bool CheckSendBval(const Digest& hash) {
        if (rc_->GetSentMsgCount() == 0) {
            return false;
        }
        auto msg = rc_->GetLastSentMsg();
        if (msg->Index() != ConsensusMessageType::BvalMessage) {
            return false;
        }
        auto bval_msg = msg->BvalData();
        if (bval_msg == nullptr) {
            return false;
        }
        return bval_msg->proposer_id.Acquire() == proposer_node_ && bval_msg->seq == seq_
               && bval_msg->hash.Acquire() == hash;
    }

    bool CheckBroadcastBval(const Digest& hash) {
        if (rc_->GetBroadcastMsgCount() == 0) {
            return false;
        }
        auto msg = rc_->GetLastBroadcastMsg();
        return IsBvalMessage(msg, hash);
    }

    bool CheckSendProm(const Digest& hash) {
        if (rc_->GetSentMsgCount() == 0) {
            return false;
        }
        auto msg = rc_->GetLastSentMsg();
        if (msg->Index() != ConsensusMessageType::PromMessage) {
            return false;
        }
        auto prom_msg = msg->PromData();
        if (prom_msg == nullptr) {
            return false;
        }
        return prom_msg->proposer_id.Acquire() == proposer_node_ && prom_msg->seq == seq_
               && Digest(prom_msg->hash.Acquire()) == hash;
    }

    bool CheckBroadcastProm(const Digest& hash) {
        if (rc_->GetBroadcastMsgCount() == 0) {
            return false;
        }
        auto broadcast_msg = rc_->GetLastBroadcastMsg();
        if (broadcast_msg->Index() != ConsensusMessageType::PromMessage) {
            return false;
        }
        auto prom_msg = broadcast_msg->PromData();
        if (prom_msg == nullptr) {
            return false;
        }
        return prom_msg->seq == seq_ && prom_msg->hash.Acquire() == hash;
    }

    bool CheckBroadcastAux(const Digest& hash) {
        if (rc_->GetBroadcastMsgCount() == 0) {
            return false;
        }
        auto broadcast_msg = rc_->GetLastBroadcastMsg();
        if (broadcast_msg->Index() != ConsensusMessageType::AuxMessage) {
            return false;
        }
        auto aux_msg = broadcast_msg->AuxData();
        if (aux_msg == nullptr) {
            return false;
        }
        return aux_msg->seq == seq_ && aux_msg->hash.Acquire() == hash;
    }

    bool CheckBroadcastAggrMainVote() {
        if (rc_->GetBroadcastMsgCount() == 0) {
            return false;
        }
        auto broadcast_msg = rc_->GetLastBroadcastMsg();
        if (broadcast_msg->Index() != ConsensusMessageType::AggregatedMainVoteMessage) {
            return false;
        }
        auto aggregated_main_vote_msg = broadcast_msg->AggregatedMainVoteData();
        if (aggregated_main_vote_msg == nullptr) {
            return false;
        }
        return true;
    }

    [[maybe_unused]] bool CheckMyBAStateA1() {
        return true;
    }

    [[maybe_unused]] bool CheckMyBAStateA2(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return endorsed_[seq_].count(proposer_node_) && endorsed_[seq_][proposer_node_] == hash;
    }

    [[maybe_unused]] bool CheckMyBAStateA3() {
        // has 2f bval1
        auto proposal = ProposalKey(seq_, proposer_node_);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        auto msgs = msg_pool->GetBvalMessages(0, proposal_hash_);
        if (msgs.size() < 4) {
            return false;
        }
        return true;
    }

    [[maybe_unused]] bool CheckMyBAStateA4() {
        // has 2f bval0 / bval1
        auto proposal = ProposalKey(seq_, proposer_node_);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        auto msgs_0 = msg_pool->GetBvalMessages(0, ZERO_32_BYTES);
        auto msgs_1 = msg_pool->GetBvalMessages(0, proposal_hash_);
        if (msgs_1.size() + msgs_0.size() < 4) {
            return false;
        }
        return true;
    }

    [[maybe_unused]] bool CheckMyBAStateA5() {
        // has 2f bval0
        auto proposal = ProposalKey(seq_, proposer_node_);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        auto msgs = msg_pool->GetBvalMessages(0, ZERO_32_BYTES);
        if (msgs.size() < 4) {
            return false;
        }
        return true;
    }

    [[maybe_unused]] bool CheckMyBAStateB1() {
        return !CheckIsNormalPath() && CheckSendBval(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateB2() {
        if (!CheckIsNormalPath()) {
            return false;
        }
        auto msgs = rc_->GetBroadcastMsgs();
        if (msgs.size() < 2) {
            return false;
        }
        bool order1 =
            IsBvalMessage(msgs[0], proposal_hash_) && IsBvalMessage(msgs[1], ZERO_32_BYTES);
        bool order2 =
            IsBvalMessage(msgs[0], ZERO_32_BYTES) && IsBvalMessage(msgs[1], proposal_hash_);
        return order1 || order2;
    }

    [[maybe_unused]] bool CheckMyBAStateB3() {
        return CheckIsNormalPath() && CheckBroadcastBval(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateC1() {
        return !CheckIsNormalPath() && CheckSendProm(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateC2() {
        return CheckIsNormalPath() && CheckBroadcastProm(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateC3() {
        return CheckIsNormalPath() && CheckBroadcastAux(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateC4() {
        return CheckIsNormalPath() && CheckBroadcastAux(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateC5() {
        return CheckIsNormalPath() && CheckBroadcastProm(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateD1() {
        return CheckIsDecided() && CheckBroadcastAggrMainVote();
    }

    [[maybe_unused]] bool CheckMyBAStateD2() {
        return CheckIsDecided() && CheckBroadcastAggrMainVote();
    }

    [[maybe_unused]] bool CheckMyBAStateD3(bool value = false) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(1) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateD4(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(1) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateE1(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(1) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateE2(bool value = false) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(1) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateE3(bool value = false) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(1) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateF1() {
        return CheckRound(1) && CheckBroadcastProm(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateF2() {
        return CheckRound(1) && CheckBroadcastAux(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateF3() {
        return CheckRound(1) && CheckBroadcastAux(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateF4() {
        return CheckRound(1) && CheckBroadcastProm(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateG1() {
        return CheckIsDecided() && CheckBroadcastAggrMainVote();
    }

    [[maybe_unused]] bool CheckMyBAStateG2() {
        return CheckIsDecided() && CheckBroadcastAggrMainVote();
    }

    [[maybe_unused]] bool CheckMyBAStateG3(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(2) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateG4(bool value = false) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(2) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateH1(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(2) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateH2(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(2) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateH3(bool value = false) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(2) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateI1() {
        return CheckRound(2) && CheckBroadcastProm(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateI2() {
        return CheckRound(2) && CheckBroadcastAux(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateI3() {
        return CheckRound(2) && CheckBroadcastAux(proposal_hash_);
    }

    [[maybe_unused]] bool CheckMyBAStateI4() {
        return CheckRound(2) && CheckBroadcastProm(ZERO_32_BYTES);
    }

    [[maybe_unused]] bool CheckMyBAStateJ1() {
        return CheckIsDecided() && CheckBroadcastAggrMainVote();
    }

    [[maybe_unused]] bool CheckMyBAStateJ2() {
        return CheckIsDecided() && CheckBroadcastAggrMainVote();
    }

    [[maybe_unused]] bool CheckMyBAStateJ3(bool value = false) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(3) && CheckBroadcastBval(hash);
    }

    [[maybe_unused]] bool CheckMyBAStateJ4(bool value = true) {
        Digest hash = value ? proposal_hash_ : ZERO_32_BYTES;
        return CheckRound(3) && CheckBroadcastBval(hash);
    }
};
}  // namespace consensus_spec