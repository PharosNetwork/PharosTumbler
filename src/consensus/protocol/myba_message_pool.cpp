// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/protocol/myba_message_pool.h"

namespace consensus_spec {

MyBAMessagePool::MyBAMessagePool(const NodeId& my_id,
                                 const Seq seq,
                                 const NodeId& proposer,
                                 std::shared_ptr<CryptoHelper> crypto_helper)
        : my_id_(my_id),
          seq_(seq),
          proposer_(proposer),
          current_round_(0),
          decided_(false),
          legacy_completed_(false),
          normal_path_(false),
          crypto_helper_(crypto_helper) {
    ba_start_time_ = time_utils::GetSteadyTimePoint();
}

bool MyBAMessagePool::ExistBval(const Digest& hash, const Round round, const NodeId& sender) {
    return bval_.count(round) && bval_[round].count(hash) && bval_[round][hash].count(sender);
}

bool MyBAMessagePool::ExistAggregatedBval(const Round round) {
    if (aggregated_bval_msg_.find(round) != aggregated_bval_msg_.end()) {
        return true;
    }
    return false;
}

bool MyBAMessagePool::ExistAggregatedMainVote(const Round round) {
    if (aggregated_mainvote_msg_.find(round) != aggregated_mainvote_msg_.end()) {
        return true;
    }
    return false;
}

bool MyBAMessagePool::ExistProm(const Digest& hash, const Round round, const NodeId& sender) {
    return prom_.count(round) && prom_[round].count(hash) && prom_[round][hash].count(sender);
}

bool MyBAMessagePool::ExistAux(const Digest& hash, const Round round, const NodeId& sender) {
    return aux_.count(round) && aux_[round].count(hash) && aux_[round][hash].count(sender);
}

uint64_t MyBAMessagePool::GetBvalBalance(const Digest& hash,
                                         const Round round,
                                         const IdToBalanceMap& peers_balance) {
    uint64_t balance = 0;
    for (const auto& sender_sig : bval_[round][hash]) {
        balance += peers_balance.at(sender_sig.first);
    }
    return balance;
}

uint16_t MyBAMessagePool::GetPromCount(const Digest& hash, const Round round) {
    return prom_[round][hash].size();
}

void MyBAMessagePool::AddBvalMsg(const NodeId& sender, const BvalMessagePtr msg) {
    bval_msgs_[msg->round][Digest(msg->hash.Acquire())][sender] = msg;
    AddBvalSignature(sender,
                     msg->round,
                     Digest(msg->hash.Acquire()),
                     Signature(msg->signature.Acquire()));
}

void MyBAMessagePool::AddBvalSignature(const NodeId& sender,
                                       const Round round,
                                       const Digest& hash,
                                       const Signature& bval_signature) {
    bval_[round][hash][sender] = bval_signature;
    aggregated_bval_aggr_sig_[round][hash].unverified_sigs.emplace(sender, bval_signature);
}

void MyBAMessagePool::AddAggregatedBvalMsg(const NodeId& sender,
                                           const AggregatedBvalMessagePtr msg) {
    aggregated_bval_msg_[msg->round] = msg;
}

void MyBAMessagePool::AddPromMsg(const NodeId& sender, const PromMessagePtr msg) {
    prom_[msg->round][Digest(msg->hash.Acquire())][sender] = Signature(msg->signature.Acquire());
    prom_msgs_[msg->round][Digest(msg->hash.Acquire())][sender] = msg;
    aggregated_mainvote_aggr_sig_[msg->round][Digest(
        msg->hash.Acquire())][AggregatedMainVoteType::Prom]
        .unverified_sigs.emplace(sender, Signature(msg->signature.Acquire()));
}

void MyBAMessagePool::AddPromMsg(const NodeId& sender,
                                 const Round round,
                                 const Digest& hash,
                                 const Signature& prom_signature) {
    prom_[round][hash][sender] = prom_signature;
}

void MyBAMessagePool::AddAuxMsg(const NodeId& sender, const AuxMessagePtr msg) {
    aux_[msg->round][Digest(msg->hash.Acquire())][sender] = msg;
    aggregated_mainvote_aggr_sig_[msg->round][Digest(
        msg->hash.Acquire())][AggregatedMainVoteType::Aux]
        .unverified_sigs.emplace(sender, Signature(msg->signature.Acquire()));
}

void MyBAMessagePool::AddAggregatedMainVoteMsg(const NodeId& sender,
                                               const AggregatedMainVoteMessagePtr msg) {
    aggregated_mainvote_msg_[msg->round] = msg;
}

bool MyBAMessagePool::HaveBvalOther(const Round round, const Digest& hash) {
    for (auto& it : bval_[round]) {
        if (it.first == hash) {
            continue;
        }
        if (it.second.count(my_id_)) {
            return true;
        }
    }
    return false;
}

std::pair<bool, BvalMessagePtr> MyBAMessagePool::GetBvalOtherMessage(const Round round,
                                                                     const Digest& hash) {
    if (bval_msgs_.find(round) == bval_msgs_.end()) {
        return std::make_pair(false, nullptr);
    }
    for (auto& it : bval_msgs_[round]) {
        if (it.first == hash) {
            continue;
        }
        auto itr2 = it.second.find(my_id_);
        if (itr2 != it.second.end()) {
            return std::make_pair(true, itr2->second);
        }
    }
    return std::make_pair(false, nullptr);
}

std::pair<bool, PromMessagePtr> MyBAMessagePool::HavePromAny(const Round round) {
    if (prom_.find(round) == prom_.end()) {
        return std::make_pair(false, nullptr);
    }
    for (auto& it : prom_[round]) {
        if (it.second.count(my_id_)) {
            return std::make_pair(true, prom_msgs_[round][it.first][my_id_]);
        }
    }
    return std::make_pair(false, nullptr);
}

std::pair<bool, AuxMessagePtr> MyBAMessagePool::HaveAuxAny(const Round round) {
    if (aux_.find(round) == aux_.end()) {
        return std::make_pair(false, nullptr);
    }
    for (auto& it : aux_[round]) {
        auto itr2 = it.second.find(my_id_);
        if (itr2 != it.second.end()) {
            return std::make_pair(true, itr2->second);
        }
    }
    return std::make_pair(false, nullptr);
}

void MyBAMessagePool::GetValidValues(const Round round, std::set<Digest>& valid_values) {
    valid_values = valid_values_[round];
}

bool MyBAMessagePool::ExistValidValue(const Round round, const Digest& hash) {
    return valid_values_.count(round) && valid_values_[round].count(hash);
}

void MyBAMessagePool::AddValidValue(const Round round, const Digest& hash) {
    valid_values_[round].insert(hash);
}

bool MyBAMessagePool::GetLatestAggregatedMainVoteMsg(AggregatedMainVoteMessagePtr& msg) {
    if (aggregated_mainvote_msg_.empty()) {
        return false;
    }
    msg = aggregated_mainvote_msg_.rbegin()->second;
    return msg != nullptr;
}

bool MyBAMessagePool::GetDecisionAggregatedMainVoteMsg(AggregatedMainVoteMessagePtr& msg) {
    if (!IsDecided()) {
        return false;
    }
    msg = decision_certificate_;
    return msg != nullptr;
}

const std::unordered_map<NodeId, BvalMessagePtr>& MyBAMessagePool::GetBvalMessages(
    const Round round,
    const Digest& hash) {
    return bval_msgs_[round][hash];
}

AggregatedBvalMessagePtr MyBAMessagePool::GetAggregatedBvalMessage(const Round round) {
    return aggregated_bval_msg_[round];
}

const std::unordered_map<NodeId, PromMessagePtr>& MyBAMessagePool::GetPromMessages(
    const Round round,
    const Digest& hash) {
    return prom_msgs_[round][hash];
}

const std::unordered_map<NodeId, AuxMessagePtr>& MyBAMessagePool::GetAuxMessages(
    const Round round,
    const Digest& hash) {
    return aux_[round][hash];
}

Seq MyBAMessagePool::GetSeq() {
    return seq_;
}

Round MyBAMessagePool::GetCurrentRound() {
    return current_round_;
}

NodeId MyBAMessagePool::GetProposer() {
    return proposer_;
}

void MyBAMessagePool::IncreaseRound(const Round r) {
    current_round_ = r;
}

void MyBAMessagePool::Decide(const Digest& hash, AggregatedMainVoteMessagePtr aggregated_mainvote) {
    decide_value_ = hash;
    decided_ = true;
    decision_certificate_ = aggregated_mainvote;
}

void MyBAMessagePool::LegacyComplete() {
    legacy_completed_ = true;
}

void MyBAMessagePool::SwitchToNormalPath() {
    normal_path_ = true;
}

bool MyBAMessagePool::IsDecided() {
    return decided_;
}

bool MyBAMessagePool::IsLegacyCompleted() {
    return legacy_completed_;
}

bool MyBAMessagePool::IsNormalPath() {
    return normal_path_;
}

double MyBAMessagePool::GetDuration() {
    return time_utils::GetDuration(ba_start_time_);
}

void MyBAMessagePool::UpdateAggregatedBvalAggrSignature(const Round round,
                                                        const Digest& hash,
                                                        const Signature& aggr_sig,
                                                        const std::vector<NodeId>& good_nodes) {
    if (aggregated_bval_aggr_sig_[round][hash].good_nodes.size() == 0) {
        aggregated_bval_aggr_sig_[round][hash].aggr_sig = aggr_sig;
    } else {
        crypto_helper_->AggregateSignature(aggregated_bval_aggr_sig_[round][hash].aggr_sig,
                                           aggr_sig);
    }
    aggregated_bval_aggr_sig_[round][hash].good_nodes.insert(good_nodes.begin(), good_nodes.end());
}

Signature MyBAMessagePool::GetAggregatedBvalAggrSignature(const Digest& hash, const Round round) {
    return aggregated_bval_aggr_sig_[round][hash].aggr_sig;
}

const std::set<NodeId>& MyBAMessagePool::GetAggregatedBvalGoodNodes(const Digest& hash,
                                                                    const Round round) {
    return aggregated_bval_aggr_sig_[round][hash].good_nodes;
}

std::map<NodeId, Signature>& MyBAMessagePool::GetAggregatedBvalUnverifiedSigs(const Digest& hash,
                                                                              const Round round) {
    return aggregated_bval_aggr_sig_[round][hash].unverified_sigs;
}

void MyBAMessagePool::UpdateMainAggrSignature(const Round round,
                                              const Digest& hash,
                                              const AggregatedMainVoteType type,
                                              const Signature& aggr_sig,
                                              const std::vector<NodeId>& good_nodes) {
    if (aggregated_mainvote_aggr_sig_[round][hash][type].good_nodes.size() == 0) {
        aggregated_mainvote_aggr_sig_[round][hash][type].aggr_sig = aggr_sig;
    } else {
        crypto_helper_->AggregateSignature(
            aggregated_mainvote_aggr_sig_[round][hash][type].aggr_sig,
            aggr_sig);
    }
    aggregated_mainvote_aggr_sig_[round][hash][type].good_nodes.insert(good_nodes.begin(),
                                                                       good_nodes.end());
}

Signature MyBAMessagePool::GetMainAggrSignature(const Digest& hash,
                                                const Round round,
                                                const AggregatedMainVoteType type) {
    return aggregated_mainvote_aggr_sig_[round][hash][type].aggr_sig;
}

const std::set<NodeId>& MyBAMessagePool::GetMainGoodNodes(const Digest& hash,
                                                          const Round round,
                                                          const AggregatedMainVoteType type) {
    return aggregated_mainvote_aggr_sig_[round][hash][type].good_nodes;
}

std::map<NodeId, Signature>& MyBAMessagePool::GetMainUnverifiedSigs(
    const Digest& hash,
    const Round round,
    const AggregatedMainVoteType type) {
    return aggregated_mainvote_aggr_sig_[round][hash][type].unverified_sigs;
}

}  // namespace consensus_spec
