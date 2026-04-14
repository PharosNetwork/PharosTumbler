// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstdlib>
#include <memory>
#include <optional>

#include "consensus/common/consensus_common.h"
#include "consensus/common/consensus_stats.h"
#include "consensus/common/metrics.h"
#include "consensus/libraries/utils/time_utils.h"
#include "consensus/protocol/myba.h"

namespace consensus_spec {

enum CheckSanityType {
    SELF_PROPOSED = 1,
    RECEIVED_FROM_OTHERS = 2,
    PULLED_FROM_OTHERS = 3,
    RECOVERED = 4,
};

using CheckProposalValidityFunc = std::function<
    bool(Seq, const NodeId&, const std::pair<uint32_t, uint32_t>, std::shared_ptr<ABuffer>)>;
using ConsensusFinishCallbackFunc =
    std::function<std::pair<bool, std::map<consensus_spec::wal::EntryType, std::vector<bytes>>>(
        Seq,
        uint64_t,
        Digest,
        std::vector<ssz_types::VoteStatus>&)>;
using SendFunc = std::function<void(const NodeId&, const MessagePtr)>;
using BroadcastFunc = std::function<void(const MessagePtr)>;

using BlockProofBuffer = std::string;
using HandleBlockProofFunc = std::function<void(std::vector<BlockProofBuffer> stable_proofs,
                                                std::vector<BlockProofBuffer> checkpoints)>;

struct ProposeState {
    std::atomic<bool> is_proposed_{false};
    std::atomic<Seq> propose_seq_{0};
    std::atomic<Seq> last_failed_seq_{0};

    void SetProposeState(bool is_proposed, Seq propose_seq, Seq last_failed_seq) {
        // Store propose_seq and last_failed_seq before is_proposed (release barrier).
        // CanPropose reads is_proposed first (acquire), ensuring it sees the other two.
        propose_seq_.store(propose_seq, std::memory_order_relaxed);
        last_failed_seq_.store(last_failed_seq, std::memory_order_relaxed);
        is_proposed_.store(is_proposed, std::memory_order_release);
    }

    void AdvanceProposeState(bool is_proposed, Seq propose_seq) {
        Seq curr = propose_seq_.load(std::memory_order_relaxed);
        if (propose_seq < curr || (propose_seq == curr && !is_proposed)) {
            return;
        }
        // Store propose_seq before is_proposed (release) so CanPropose sees consistent state.
        propose_seq_.store(propose_seq, std::memory_order_relaxed);
        is_proposed_.store(is_proposed, std::memory_order_release);
    }
};

class MyTumblerEngineBase {
  public:
    MyTumblerEngineBase();
    ~MyTumblerEngineBase();

    void InitCryptoFunc(DigestFunc digest_f,
                        std::shared_ptr<ECCSigner> ecc_signer,
                        std::shared_ptr<BLSAggregator> aggregator);

    bool Configure(const uint16_t spec_version,
                   const uint32_t epoch_number,
                   const NodeId& my_id,
                   const std::shared_ptr<IdToPublicKeyMap> peers,
                   const std::shared_ptr<IdToBalanceMap> peers_balance,
                   const MyTumblerConfig& cfg,
                   const Seq last_consensus_seq,
                   const Timestamp last_consensus_ts,
                   const Seq last_stable_seq,
                   const bool is_epoch_end,
                   const std::map<Seq, std::set<NodeId>>& proposers_map,
                   std::shared_ptr<wal::WalLocalStorage> db,
                   SendFunc send_f,
                   BroadcastFunc broadcast_f,
                   CheckProposalValidityFunc check_validity,
                   ConsensusFinishCallbackFunc consensus_finish_cb,
                   HandleBlockProofFunc handle_block_proof_cb);

    // start mytumbler engine base
    void Start();

    // stop mytumbler engine base
    void Stop();

    // handle mytumbler messages, including myba messages and val/pass/skip/request/response
    void OnConsensusMessage(const NodeId& sender, const MessagePtr msg);

    // check if can propose, returns state and last failed sequence
    std::pair<MyTumblerProposeState, Seq> CanPropose();

    // make a propose
    void Propose(bytes proposal,
                 bool is_empty,
                 const Timestamp timestamp,
                 std::promise<std::pair<MyTumblerProposeState, Seq>>* prom);

    // update sync raw sequence
    void UpdateSyncRawSeq(const Seq seq, const Timestamp timestamp, const bool is_epoch_end);

    // update stable to advance consensus window
    void UpdateStableSeq(const Seq seq, std::set<NodeId> next_proposers);
    // update finished sequence to advance consensus window
    void UpdateFinishedSequence(const Seq seq, std::vector<std::string> block_checkpoints);

    /// Aggregates counters for external RPC. Intended callers: **integration / RPC threads only**
    /// (not handlers running on `consensus_io_ctx_`). Implementation uses `post` + blocking
    /// `future.get()` like `Propose()`; if this were called from inside a consensus handler, the
    /// posted task would wait behind the current handler while this thread blocks in `get()` —
    /// **deadlock**. Today only external RPC invokes this, which is safe.
    std::pair<ConsensusStatsError, std::optional<ConsensusStats>> GetConsensusStats() const;

    // trigger check tx done
    void CheckSanityDone(const Seq seq_number,
                         const NodeId& node_id,
                         const std::pair<uint32_t, uint32_t> index_epoch,
                         std::optional<bytes> new_proposal);

  private:
    void DoPropose(bytes proposal,
                   bool is_empty,
                   const Timestamp timestamp,
                   std::promise<std::pair<MyTumblerProposeState, Seq>>* prom);
    void EndorseCallBack(const Seq seq, const NodeId& proposer, const Digest& hash);
    void RecordVoterCallBack(const Seq seq_number,
                             const NodeId& proposer,
                             const Digest& digest,
                             const NodeId& voter);
    void MyBACompleteCallBack(const Seq seq, const NodeId& proposer, const Digest& hash);

    void ReliableBroadcastMsg(const MessagePtr msg, bool need_save = true);
    void SendMsg(const NodeId& node, const MessagePtr msg);
    void BroadcastMsg(const MessagePtr msg);
    bool CanPipeline(const Seq num);
    void DoOnRecvValMessage(const NodeId& sender,
                            const ValMessagePtr msg,
                            const CheckSanityType type);
    void DoOnRecvPassMessage(const NodeId& sender, const PassMessagePtr msg);
    void DoOnRecvSkipMessage(const NodeId& sender, const SkipMessagePtr msg);
    void DoOnRecvForwardSkipMessage(const NodeId& sender, const ForwardSkipMessagePtr msg);
    void DoEndorseCallBack(const Seq seq, const NodeId& proposer, const Digest& hash);
    void DoMyBACompleteCallBack(const Seq seq, const NodeId& proposer, const Digest& hash);
    void CheckCanPass();
    void CheckCanSkip();
    void Pass();
    void CheckPassStatus();
    void CheckConsensusComplete();
    // std::pair<bool, Digest> CheckTermStatus(const Digest& d);
    void DoOnConsensusComplete(const Seq seq, const Digest& random_seed = Digest());
    void DoUpdateRawSeq(const Seq seq,
                        const Timestamp timestamp,
                        bool is_sync,
                        const bool is_epoch_end);
    void DoUpdateFinishedSequence(const Seq seq, std::vector<std::string> block_checkpoints);
    void DoUpdateStableSeq(const Seq seq, std::set<NodeId> next_proposers);
    bool CheckMessageSanity(Seq seq_number,
                            const NodeId& propose_node,
                            CheckSanityType type,
                            const ValMessagePtr proposal);
    void SetNode();
    void HandlePendingMessage(const Seq seq);
    bool SetConsensusThread(const uint16_t num);
    Seq GetCurrSeq();
    NodeId GetLeader(const Seq seq);
    MyBAPtr GetMyBA(const NodeId& proposer);
    void GarbageCollection(const Seq seq);
    void ClearCurrSeqState(const Seq seq);

    void DoOnRequestProposalTimeout(const asio::error_code& code, bool cancel);
    void DoOnSkipSendTimeout(const asio::error_code& code, bool cancel);
    void DoOnForwardSkipTimeout(const asio::error_code& code, bool cancel);
    void DoOnResendProofTimeout(const asio::error_code& code, bool cancel);
    void DoOnHardPassTimeout(const asio::error_code& code, bool cancel);
    void DoOnSoftPassTimeout(const asio::error_code& code, bool cancel);
    int DoOnRecvRequestProposalMessage(const NodeId& sender, const RequestProposalMessagePtr msg);
    int DoOnRecvResponseProposalMessage(const NodeId& sender, const ResponseProposalMessagePtr msg);
    void DoOnCheckSanityDone(const Seq seq_number,
                             const NodeId& node_id,
                             const std::pair<uint32_t, uint32_t> index_epoch,
                             std::optional<bytes> new_proposal);
    void DoRecordVoter(const Seq seq_number,
                       const NodeId& proposer,
                       const Digest& digest,
                       const NodeId& voter);
    void BroadcastProofs();
    void PrintDiagnostics();

    // for metrics
    void UpdateConsensusStartTime(const Seq seq);
    void UpdateConsensusFinishMetrics(const Seq seq);

    std::pair<ConsensusStatsError, std::optional<ConsensusStats>> BuildConsensusStatsSnapshot()
        const;

  public:
    // read only
    NodeId my_id_;
    std::shared_ptr<IdToPublicKeyMap> peers_;
    std::shared_ptr<IdToBalanceMap> peers_balance_;
    uint16_t n_{0};
    // uint16_t f_;
    MyTumblerConfig cfg_;

  private:
    uint16_t spec_version_{0};
    uint32_t epoch_number_{0};
    uint64_t total_balance_{0};
    uint64_t tolerable_faulty_balance_{0};
    uint64_t quorum_balance_{0};
    std::shared_ptr<CryptoHelper> crypto_helper_{nullptr};
    // v2 limit proposers:
    std::set<NodeId> current_proposers_;
    std::atomic<bool> is_proposer_{true};
    std::map<Seq, std::set<NodeId>> next_proposers_;
    mutable Mutex inactive_proposers_lock_;
    std::set<NodeId> inactive_proposers_;
    std::map<NodeId, uint64_t> peer_inactive_events_;

    bool stopped_{true};
    std::atomic<Timestamp> last_consensus_ts_{0};
    std::atomic<Seq> last_consensus_seq_{0};
    std::atomic<Seq> last_stable_seq_{0};
    Timestamp last_stable_update_time_{0};
    // std::atomic<bool> in_consensus_{false};
    std::atomic<bool> is_epoch_end_{false};
    ProposeState propose_state_;
    bool delay_pass_{true};
    std::vector<MyBAPtr> myba_;
    ReliableChannelBasePtr reliable_channel_;

    std::unique_ptr<std::thread> consensus_thread_;
    std::unique_ptr<asio::io_context> consensus_io_ctx_;

    RecursiveMutex pending_msg_lock_;
    std::map<PersistentKey, MessagePtr> pending_msg_;

    // proposals waiting to be checked
    std::map<Seq,
             std::unordered_map<NodeId, std::vector<std::pair<CheckSanityType, ValMessagePtr>>>>
        unchecked_proposals_;

    // all vals may be included
    // can be cleared when update stable
    std::map<Seq, std::unordered_map<NodeId, std::unordered_map<Digest, ValMessagePtr>>> val_;

    // status of curr seq's consensus
    // can be cleared when update raw
    std::map<Seq, std::set<NodeId>> pending_;  // myba not decided, or result=1 but val not received
    std::set<NodeId> already_vote_zero_;       // I have voted zero for these proposers
    std::set<NodeId> ba_complete_;             // myba completed, result may be 1 or 0
    std::map<NodeId, Digest> ba_success_;      // myba consensus=1, needs to be ordered
    std::map<Seq, std::set<NodeId>> endorsed_;  // I have endorsed for these proposers
    std::unordered_map<NodeId, Signature> skipped_;
    std::unordered_map<NodeId, PassMessagePtr> passed_;
    uint64_t passed_balance_{0};
    std::set<NodeId> output_;  // skipped_ + ba_complete_
    uint64_t output_balance_{0};
    std::set<NodeId> silent_this_round_;  // no val nor skip this round

    std::unordered_map<NodeId, std::unordered_map<Digest, std::list<NodeId>>> voters_with_tx_;

    MyTimer request_proposal_timer_;  // for request vals if needed
    MyTimer skip_send_timer_;         // since last consensus complete, if has BA success then skip
    bool can_skip_{false};            // send skip only when can_skip_ = true
    MyTimer forward_skip_timer_;      // for forward skip if stuck in pass

    // v1 only
    Seq propose_empty_turn_{0};

    // v2 only
    MyTimer soft_pass_timer_;   // since first BA success
    MyTimer hard_pass_timer_;   // since last consensus complete
    bool is_hard_pass_{false};  // if hard pass timeout

    SendFunc send_;
    BroadcastFunc broadcast_;
    CheckProposalValidityFunc check_validity_;
    ConsensusFinishCallbackFunc consensus_finish_cb_;

    // for metrics
    std::map<Seq, SteadyTimePoint> consensus_start_time_;
    SteadyTimePoint pass_start_time_;
    SteadyTimePoint consensus_finish_time_;
    SteadyTimePoint last_rawblock_finish_time_;

    // for stabilize, purely visited by mytumbler thread
    Seq last_finished_seq_{0};
    std::map<Seq, BlockProofBuffer> stable_proofs_;  // the last block in sequence
    std::map<Seq, std::vector<BlockProofBuffer>> my_finished_proofs_;
    Seq last_sent_proof_seq_{0};
    HandleBlockProofFunc handle_block_proof_cb_;
    MyTimer resend_proof_timer_;  // to flush all proofs until epoch stabilized

    bool engine_stats_configured_{false};
    Seq stats_baseline_last_consensus_seq_{0};
    uint64_t sync_completed_count_{0};
    uint64_t proposal_success_count_{0};
    uint64_t proposal_rejected_count_{0};
    uint64_t skip_stats_count_{0};
};

using MyTumblerEngineBasePtr = std::shared_ptr<MyTumblerEngineBase>;

}  // namespace consensus_spec
