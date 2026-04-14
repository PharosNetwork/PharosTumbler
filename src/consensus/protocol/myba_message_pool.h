// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <map>
#include <unordered_map>
#include <set>

#include "consensus/libraries/utils/time_utils.h"
#include "consensus/libraries/common/type_define.h"
#include "consensus/protocol/mytumbler_message_types.h"

namespace consensus_spec {

template <typename T>
using MessageMap =
    std::unordered_map<Round, std::unordered_map<Digest, std::unordered_map<NodeId, T>>>;

struct AggregateSignatureContext {
    Signature aggr_sig;
    std::set<NodeId> good_nodes;
    std::map<NodeId, Signature> unverified_sigs;
};

class MyBAMessagePool {
    friend class MyBAMessagePoolTest;

  public:
    MyBAMessagePool(const NodeId& my_id,
                    const Seq seq,
                    const NodeId& proposer,
                    std::shared_ptr<CryptoHelper> crypto_helper);
    ~MyBAMessagePool() = default;

    bool ExistBval(const Digest& hash, const Round round, const NodeId& sender);
    bool ExistAggregatedBval(const Round round);
    bool ExistProm(const Digest& hash, const Round round, const NodeId& sender);
    bool ExistAux(const Digest& hash, const Round round, const NodeId& sender);
    bool ExistAggregatedMainVote(const Round round);

    uint64_t GetBvalBalance(const Digest& hash,
                            const Round round,
                            const IdToBalanceMap& peers_balance);
    uint16_t GetPromCount(const Digest& hash, const Round round);

    void AddBvalMsg(const NodeId& sender, const BvalMessagePtr msg);
    void AddBvalSignature(const NodeId& sender,
                          const Round round,
                          const Digest& hash,
                          const Signature& bval_signature);
    void AddAggregatedBvalMsg(const NodeId& sender, const AggregatedBvalMessagePtr msg);
    void AddPromMsg(const NodeId& sender, const PromMessagePtr msg);
    void AddPromMsg(const NodeId& sender,
                    const Round round,
                    const Digest& hash,
                    const Signature& prom_signature);
    void AddAuxMsg(const NodeId& sender, const AuxMessagePtr msg);
    void AddAggregatedMainVoteMsg(const NodeId& sender, const AggregatedMainVoteMessagePtr msg);

    bool HaveBvalOther(const Round round, const Digest& hash);
    std::pair<bool, BvalMessagePtr> GetBvalOtherMessage(const Round round, const Digest& hash);
    std::pair<bool, PromMessagePtr> HavePromAny(const Round round);
    std::pair<bool, AuxMessagePtr> HaveAuxAny(const Round round);
    void GetValidValues(const Round round, std::set<Digest>& valid_values);
    bool ExistValidValue(const Round round, const Digest& hash);
    void AddValidValue(const Round round, const Digest& hash);
    bool GetLatestAggregatedMainVoteMsg(AggregatedMainVoteMessagePtr& msg);
    bool GetDecisionAggregatedMainVoteMsg(AggregatedMainVoteMessagePtr& msg);

    const std::unordered_map<NodeId, BvalMessagePtr>& GetBvalMessages(const Round round,
                                                                      const Digest& hash);

    AggregatedBvalMessagePtr GetAggregatedBvalMessage(const Round round);

    const std::unordered_map<NodeId, PromMessagePtr>& GetPromMessages(const Round round,
                                                                      const Digest& hash);

    const std::unordered_map<NodeId, AuxMessagePtr>& GetAuxMessages(const Round round,
                                                                    const Digest& hash);

    Seq GetSeq();
    Round GetCurrentRound();
    NodeId GetProposer();
    void IncreaseRound(const Round r);
    void Decide(const Digest& hash, AggregatedMainVoteMessagePtr aggregated_mainvote);
    void LegacyComplete();
    void SwitchToNormalPath();
    bool IsDecided();
    bool IsLegacyCompleted();
    bool IsNormalPath();

    double GetDuration();

    void UpdateAggregatedBvalAggrSignature(const Round round,
                                           const Digest& hash,
                                           const Signature& aggr_sig,
                                           const std::vector<NodeId>& good_nodes);
    Signature GetAggregatedBvalAggrSignature(const Digest& hash, const Round round);
    const std::set<NodeId>& GetAggregatedBvalGoodNodes(const Digest& hash, const Round round);
    std::map<NodeId, Signature>& GetAggregatedBvalUnverifiedSigs(const Digest& hash,
                                                                 const Round round);

    void UpdateMainAggrSignature(const Round round,
                                 const Digest& hash,
                                 const AggregatedMainVoteType type,
                                 const Signature& aggr_sig,
                                 const std::vector<NodeId>& good_nodes);
    Signature GetMainAggrSignature(const Digest& hash,
                                   const Round round,
                                   const AggregatedMainVoteType type);
    const std::set<NodeId>& GetMainGoodNodes(const Digest& hash,
                                             const Round round,
                                             const AggregatedMainVoteType type);
    std::map<NodeId, Signature>& GetMainUnverifiedSigs(const Digest& hash,
                                                       const Round round,
                                                       const AggregatedMainVoteType type);

  private:
    NodeId my_id_;
    Seq seq_;
    NodeId proposer_;
    Round current_round_;
    bool decided_;
    bool legacy_completed_;
    bool normal_path_;
    Digest decide_value_;

    std::shared_ptr<CryptoHelper> crypto_helper_{nullptr};

    std::unordered_map<Round, std::set<Digest>> valid_values_;
    MessageMap<Signature> bval_;
    std::unordered_map<Round, std::unordered_map<Digest, AggregateSignatureContext>>
        aggregated_bval_aggr_sig_;
    std::unordered_map<Round, AggregatedBvalMessagePtr> aggregated_bval_msg_;
    MessageMap<BvalMessagePtr> bval_msgs_;
    MessageMap<Signature> prom_;
    MessageMap<PromMessagePtr> prom_msgs_;
    MessageMap<AuxMessagePtr> aux_;
    std::unordered_map<
        Round,
        std::unordered_map<Digest,
                           std::unordered_map<AggregatedMainVoteType, AggregateSignatureContext>>>
        aggregated_mainvote_aggr_sig_;
    std::map<Round, AggregatedMainVoteMessagePtr> aggregated_mainvote_msg_;
    AggregatedMainVoteMessagePtr decision_certificate_{nullptr};

    // for metrics
    SteadyTimePoint ba_start_time_;
};

using MyBAMessagePoolPtr = std::shared_ptr<MyBAMessagePool>;

}  // namespace consensus_spec
