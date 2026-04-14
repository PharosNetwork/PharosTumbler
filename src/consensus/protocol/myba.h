// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include "consensus/libraries/thread/worker.h"
#include "consensus/libraries/thread/concurrent_queue.h"
#include "consensus/protocol/myba_message_pool.h"
#include "consensus/protocol/reliable_channel.h"
#include "consensus/protocol/mytumbler_message_types.h"

namespace consensus_spec {

using RecordVoterCallback =
    std::function<void(const Seq, const NodeId&, const Digest&, const NodeId&)>;
using EndorseCallback = std::function<void(const Seq, const NodeId&, const Digest&)>;
using MyBACompleteCallback = std::function<void(const Seq, const NodeId&, const Digest&)>;

class MyBA : public Worker {
    friend class MyBATest;

  public:
    MyBA(const uint16_t spec_version,
         const uint32_t epoch_number,
         const NodeId& my_id,
         const uint16_t n,
         const uint64_t tolerable_faulty_balance,
         const uint64_t quorum_balance,
         const Seq stable_seq,
         std::shared_ptr<CryptoHelper> crypto_helper,
         std::shared_ptr<IdToPublicKeyMap> peers,
         std::shared_ptr<IdToBalanceMap> peers_balance,
         EndorseCallback endorse_cb,
         RecordVoterCallback record_voter_cb,
         MyBACompleteCallback ba_complete_cb,
         ReliableChannelBasePtr reliable_channel);
    ~MyBA() = default;

    void LoadLog(const Seq seq,
                 const NodeId& proposer,
                 const std::vector<std::pair<NodeId, MessagePtr>>& msgs);
    void GarbageCollection(const Seq seq);

  public:
    void Start();
    void Stop();

    void PushMessage(const NodeId& sender, const MessagePtr msg);
    void Endorse(const Seq seq, const NodeId& proposer, const Digest& hash, bool val_exist);
    void SwitchToNormalPath(const Seq seq, const NodeId& proposer);

  private:
    void DoWork() override;
    int OnRecvMessage(const NodeId& sender, const MessagePtr msg);
    void DoEndorse(const Seq seq, const NodeId& proposer, const Digest& hash, bool val_exist);
    void DoSwitchToNormalPath(const Seq seq, const NodeId& proposer);
    void DoGarbageCollection(const Seq seq);

    void SendMessage(const NodeId& node_id, const MessagePtr msg);
    void BroadcastMessage(const MessagePtr msg);

    int OnRecvBvalMessage(const NodeId& sender, const MessagePtr msg);
    int OnRecvAggregatedBvalMessage(const NodeId& sender, const MessagePtr msg);
    int OnRecvPromMessage(const NodeId& sender, const MessagePtr msg);
    int OnRecvAuxMessage(const NodeId& sender, const MessagePtr msg);
    int OnRecvAggregatedMainVoteMessage(const NodeId& sender,
                                        const MessagePtr msg,
                                        bool is_restored = false);

    void CheckBvalStatus(MyBAMessagePoolPtr msg_pool, const Digest& hash);
    void CheckPhaseTwoVoteStatus(MyBAMessagePoolPtr msg_pool,
                                 const Round round,
                                 const Digest& hash);
    bool CheckCompleted(const NodeId& sender, MyBAMessagePoolPtr msg_pool);
    bool GetCoin(const Round round);
    void Decide(bool fast_path,
                const Round round,
                MyBAMessagePoolPtr msg_pool,
                const Digest& hash,
                const AggregatedMainVoteMessagePtr aggregated_mainvote);

    bool VerifyAggregatedMainVote(const AggregatedMainVoteMessagePtr msg);

    bool TryGenerateAggregatedBvalAggrSignature(MyBAMessagePoolPtr msg_pool,
                                                const Seq seq,
                                                const Round round,
                                                const Digest& hash,
                                                ssz_types::AggregateSignature& aggr_sig);
    bool TryGenerateMainVoteAggrSignature(MyBAMessagePoolPtr msg_pool,
                                          const Seq seq,
                                          const Round round,
                                          std::vector<ssz_types::AggrAuxInfo>& aux_aggr_infos,
                                          std::vector<ssz_types::AggrPromInfo>& prom_aggr_infos);

    uint16_t GetNodeIndex(const NodeId& node_id);
    MyBAMessagePoolPtr GetMsgPool(const ProposalKey& key);

    // uint16_t Threshold_quorum();
    // uint16_t Threshold_f();
    bool CheckQuorumBalance(const ssz_types::AggregateSignature& aggr_signature);
    bool VerifyPhaseOneQC(const Seq seq,
                          const NodeId& proposer_id,
                          const Round round,
                          const Digest& hash,
                          const ssz_types::AggregateSignature& aggr_signature);
    void DoPhaseTwoVote(MyBAMessagePoolPtr msg_pool,
                        const NodeId& sender,
                        const Seq seq,
                        const NodeId& proposer_id,
                        const Round round,
                        const Digest& hash,
                        const ssz_types::AggregateSignature& aggr_signature);
    bool LegacyVersionBeforeDecisionProof();

  private:
    // MyTumblerEngineBase* engine_;
    uint16_t spec_version_;
    uint32_t epoch_number_;
    NodeId my_id_;
    const uint16_t n_;
    const uint64_t tolerable_faulty_balance_{0};
    const uint64_t quorum_balance_{0};
    std::atomic<Seq> stable_seq_{0};
    // uint16_t f_;

    std::shared_ptr<CryptoHelper> crypto_helper_{nullptr};

    std::shared_ptr<IdToPublicKeyMap> peers_;
    std::shared_ptr<IdToBalanceMap> peers_balance_;
    EndorseCallback endorse_cb_;
    RecordVoterCallback record_voter_cb_;
    MyBACompleteCallback ba_complete_cb_;
    ConcurrentQueue<MessageQueueItem> msg_queue_;
    // Thread-safety: called only from myba thread
    std::map<ProposalKey, MyBAMessagePoolPtr> ba_msg_pool_;
    ReliableChannelBasePtr reliable_channel_ = nullptr;
};

using MyBAPtr = std::shared_ptr<MyBA>;

}  // namespace consensus_spec
