// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/protocol/mytumbler_engine_base.h"

#include <future>
#include <optional>

#include <pamir/cetina/cetina.h>
#include <cobre/libraries/common/macro.h>

#include "consensus/libraries/common/const_define.h"
#include "consensus/libraries/log/consensus_log.h"
#include "consensus/libraries/thread/thread_utils.h"

namespace consensus_spec {

MyTumblerEngineBase::MyTumblerEngineBase()
        : consensus_io_ctx_(std::make_unique<asio::io_context>()),
          request_proposal_timer_(*consensus_io_ctx_,
                                  1000,
                                  [this](const asio::error_code& code, bool cancel) {
                                      this->DoOnRequestProposalTimeout(code, cancel);
                                  }),
          skip_send_timer_(*consensus_io_ctx_,
                           100,
                           [this](const asio::error_code& code, bool cancel) {
                               this->DoOnSkipSendTimeout(code, cancel);
                           }),
          forward_skip_timer_(*consensus_io_ctx_,
                              400,
                              [this](const asio::error_code& code, bool cancel) {
                                  this->DoOnForwardSkipTimeout(code, cancel);
                              }),
          soft_pass_timer_(*consensus_io_ctx_,
                           500,
                           [this](const asio::error_code& code, bool cancel) {
                               this->DoOnSoftPassTimeout(code, cancel);
                           }),
          hard_pass_timer_(*consensus_io_ctx_,
                           5000,
                           [this](const asio::error_code& code, bool cancel) {
                               this->DoOnHardPassTimeout(code, cancel);
                           }),

          resend_proof_timer_(*consensus_io_ctx_,
                              50,
                              [this](const asio::error_code& code, bool cancel) {
                                  this->DoOnResendProofTimeout(code, cancel);
                              }) {}

MyTumblerEngineBase::~MyTumblerEngineBase() {
    if (!stopped_) {
        Stop();
    }
}

void MyTumblerEngineBase::InitCryptoFunc(DigestFunc digest_f,
                                         std::shared_ptr<ECCSigner> ecc_signer,
                                         std::shared_ptr<BLSAggregator> aggregator) {
    crypto_helper_ = std::make_shared<CryptoHelper>();
    crypto_helper_->InitCryptoFunc(digest_f, ecc_signer, aggregator);
}

bool MyTumblerEngineBase::Configure(const uint16_t spec_version,
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
                                    HandleBlockProofFunc handle_block_proof_cb) {
    CS_LOG_INFO("Init mytumbler engine with version {}, my id is {}, epoch number {}",
                spec_version,
                toHex(my_id).substr(0, 8),
                epoch_number);

    if (spec_version < CONSENSUS_VERSION_MIN) {
        CS_LOG_ERROR("spec_version {} is not supported, minimum required is {}",
                     spec_version,
                     CONSENSUS_VERSION_MIN);
        return false;
    }

    spec_version_ = spec_version;
    epoch_number_ = epoch_number;
    my_id_ = my_id;
    peers_ = peers;
    peers_balance_ = peers_balance;

    cfg_ = cfg;
    last_consensus_ts_ = last_consensus_ts;
    last_consensus_seq_ = last_consensus_seq;
    propose_empty_turn_ = last_consensus_seq + 1;
    last_stable_seq_.store(last_stable_seq);
    last_finished_seq_ = last_stable_seq;
    last_sent_proof_seq_ = last_stable_seq;
    is_epoch_end_.store(is_epoch_end);
    send_ = send_f;
    broadcast_ = broadcast_f;
    check_validity_ = check_validity;
    consensus_finish_cb_ = consensus_finish_cb;
    handle_block_proof_cb_ = handle_block_proof_cb;
    // has_termed_ = std::make_pair(false, Digest());

    request_proposal_timer_.ResetTimeoutInterval(cfg_.reliable_channel_resend_interval_);
    soft_pass_timer_.ResetTimeoutInterval(cfg_.pace_keeping_interval_);
    skip_send_timer_.ResetTimeoutInterval(cfg_.pace_keeping_interval_ / 2);  // default 250ms
    forward_skip_timer_.ResetTimeoutInterval(cfg_.reliable_channel_resend_interval_);
    resend_proof_timer_.ResetTimeoutInterval(cfg_.broadcast_proof_interval_);

    // V7+ only: limited proposers from proposers_map
    if (auto itr = proposers_map.upper_bound(GetCurrSeq()); itr == proposers_map.begin()) {
        CS_LOG_ERROR("no proposers specified for {}", GetCurrSeq());
        return false;
    } else {
        --itr;
        current_proposers_ = itr->second;
        is_proposer_.store(current_proposers_.count(my_id_));
        next_proposers_ = proposers_map;
    }

    // reliable channel
    reliable_channel_ = std::make_shared<ReliableChannel>(
        spec_version_,
        my_id_,
        cfg.reliable_channel_resend_timeout_,
        cfg.reliable_channel_resend_interval_,
        [this](const NodeId& node, const MessagePtr msg) {
            SendMsg(node, msg);
        },
        [this](const MessagePtr msg) {
            BroadcastMsg(msg);
        });
    if (!reliable_channel_->ConfigureDB(cfg_.need_persist_message_, db)) {
        return false;
    }

    SetNode();

    // thread pool for myba
    size_t threads_max = std::min(peers_->size(), static_cast<size_t>(MYTUMBLER_THREADS_MAX));
    if (cfg_.threads_ < 1 || cfg_.threads_ > threads_max) {
        cfg_.threads_ = threads_max;
    }
    if (!SetConsensusThread(cfg_.threads_)) {
        return false;
    }

    propose_state_.SetProposeState(false, last_consensus_seq_ + 1, 0);

    // Init state info
    // 1. resend msg among (last_stable_seq, last_consensus_seq]
    for (auto seq = last_stable_seq_ + 1; seq <= last_consensus_seq_; ++seq) {
        reliable_channel_->ResendMessages(seq);
    }
    // 2. reload msg of last_consensus_seq+1, maybe also vals from seq+2
    if (cfg_.need_persist_message_) {
        std::unordered_map<NodeId, std::vector<std::pair<NodeId, MessagePtr>>> myba_msgs;
        std::vector<ValMessagePtr> vals;
        std::vector<PassMessagePtr> pass_msgs;
        passed_.clear();
        passed_balance_ = 0;
        endorsed_.clear();
        CS_LOG_DEBUG("reliable channel will load data from db");
        reliable_channel_->LoadMessages(last_consensus_seq_ + 1,
                                        last_consensus_seq_ + 2,
                                        epoch_number,
                                        vals,
                                        skipped_,
                                        myba_msgs,
                                        endorsed_,
                                        pass_msgs);

        CS_LOG_INFO(
            "load data from db: val size: {}, skip size: {}, instance size: {}, has pass: {}",
            vals.size(),
            skipped_.size(),
            myba_msgs.size(),
            !pass_msgs.empty());

        if (!pass_msgs.empty()) {
            // must be my own. broadcast pass msg
            auto pass_msg = pass_msgs[0];
            propose_state_.SetProposeState(false, last_consensus_seq_ + 2, 0);
            consensus_io_ctx_->post([this, pass_msg]() {
                ReliableBroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(pass_msg),
                                     false);
                CS_LOG_INFO("broadcast pass {}", pass_msg->seq);
                DoOnRecvPassMessage(my_id_, pass_msg);

                // update cur_seq endorsed set according to my pass msg
                endorsed_[pass_msg->seq].clear();
                for (auto& proposer : pass_msg->endorsed.Acquire()) {
                    if (peers_->find(NodeId(proposer.Acquire())) == peers_->end()) {
                        CS_LOG_ERROR("invalid proposer in my pass endorse");
                        return;
                    }
                    endorsed_[pass_msg->seq].emplace(NodeId(proposer.Acquire()));
                }

                // start forward skip timer
                forward_skip_timer_.Reset();
                pass_start_time_ = time_utils::GetSteadyTimePoint();
            });
        }

        for (const auto& [node_id, skip_data] : skipped_) {
            CS_LOG_INFO("already have skip from {}", toHex(node_id).substr(0, 8));
            if (auto [iter, inserted] = output_.insert(node_id); inserted) {
                output_balance_ += peers_balance_->at(node_id);
            }
            if (node_id == my_id_) {
                propose_state_.SetProposeState(false, last_consensus_seq_ + 2, 0);
            }
        }
        consensus_io_ctx_->post([this] {
            CheckCanPass();
        });

        for (auto& msg : myba_msgs) {
            GetMyBA(msg.first)->LoadLog(last_consensus_seq_ + 1, msg.first, msg.second);
        }

        // handle val after restoring all ba(s)
        for (auto val : vals) {
            // there are seq + 1 and seq + 2 vals
            if (val->epoch_number != epoch_number_) {
                CS_LOG_INFO("load val from epoch {} not match current epoch {}",
                            val->epoch_number,
                            epoch_number_);
                continue;
            }
            if (NodeId(val->proposer_id.Acquire()) == my_id_) {
                // in_consensus_ = true;
                if (val->seq >= propose_state_.propose_seq_.load(std::memory_order_relaxed)) {
                    propose_state_.AdvanceProposeState(true, val->seq);
                }
            }
            CS_LOG_INFO("already have val from {} hash {}",
                        toHex(val->proposer_id.Acquire()).substr(0, 8),
                        toHex(val->hash.Acquire()).substr(0, 8));
            consensus_io_ctx_->post([this, val]() {
                this->DoOnRecvValMessage(NodeId(val->proposer_id.Acquire()),
                                         val,
                                         CheckSanityType::RECOVERED);
            });
        }
    }  // end reload messages

    consensus_start_time_[GetCurrSeq()] = time_utils::GetSteadyTimePoint();
    last_rawblock_finish_time_ = time_utils::GetSteadyTimePoint();

    stats_baseline_last_consensus_seq_ = last_consensus_seq;
    engine_stats_configured_ = true;

    CS_LOG_INFO("Init mytumbler engine successfully");
    return true;
}

void MyTumblerEngineBase::Start() {
    CS_LOG_INFO("start mytumbler engine ...");

    consensus_thread_ = std::make_unique<std::thread>([&]() {
        thread_utils::SetCurrentThreadName("mytumbler_engine_base");
        CS_LOG_INFO("mytumbler consensus thread started");
        auto work = asio::make_work_guard(*consensus_io_ctx_);
        consensus_io_ctx_->run();
        CS_LOG_INFO("mytumbler consensus thread exited");
    });

    reliable_channel_->Start();
    for (auto& ptr : myba_) {
        ptr->Start();
    }

    if (!is_epoch_end_.load()) {
        hard_pass_timer_.Start();
    } else {
        resend_proof_timer_.ResetTimeoutInterval(cfg_.broadcast_proof_interval_);
        resend_proof_timer_.Start();
    }
    request_proposal_timer_.Start();

    can_skip_ = false;
    if (!is_epoch_end_.load()) {
        skip_send_timer_.Reset();
    }

    stopped_ = false;
    last_stable_update_time_ = time_utils::GetCurrentTimestamp();
    CS_LOG_INFO("start mytumbler engine successfully");
}

void MyTumblerEngineBase::Stop() {
    CS_LOG_INFO("stop mytumbler engine ...");

    for (auto& ptr : myba_) {
        ptr->Stop();
    }

    if (reliable_channel_) {
        reliable_channel_->Stop();
    }

    request_proposal_timer_.Stop();

    try {
        consensus_io_ctx_->stop();
        if (consensus_thread_) {
            consensus_thread_->join();
        }
        consensus_thread_.reset();
    } catch (std::exception& e) {
        CS_LOG_ERROR("stop mytumbler consensus thread failed");
    }

    stopped_ = true;
    CS_LOG_INFO("stop mytumbler engine successfully");
}

void MyTumblerEngineBase::OnConsensusMessage(const NodeId& sender, const MessagePtr msg) {
    if (!msg) {
        return;
    }

    if (stopped_) {
        return;
    }

    if (peers_->find(sender) == peers_->end()) {
        CS_LOG_WARN("Sender not a validator");
        return;
    }

    // Unified validation: by Index() ensure the corresponding typed data pointer is non-null.
    // Rejects malformed/empty ConsensusMessage to avoid crashes on later dereference.
    MessageGenericInfo info;
    if (!GetMessageInfo(msg, info)) {
        CS_LOG_ERROR("invalid msg type {} from {}", msg->Index(), toHex(sender).substr(0, 8));
        return;
    }

    // V7+ only: epoch must match for Val/ValReceipt/Bval/BvalReceipt (data already validated above)
    bool epoch_match = true;
    if (msg->Index() == ConsensusMessageType::ValMessage) {
        epoch_match = (epoch_number_ == msg->ValData()->epoch_number);
    } else if (msg->Index() == ConsensusMessageType::ValReceiptMessage) {
        epoch_match = (epoch_number_ == msg->ValReceiptData()->epoch_number);
    } else if (msg->Index() == ConsensusMessageType::BvalMessage) {
        epoch_match = (epoch_number_ == msg->BvalData()->epoch_number);
    } else if (msg->Index() == ConsensusMessageType::BvalReceiptMessage) {
        epoch_match = (epoch_number_ == msg->BvalReceiptData()->epoch_number);
    }
    if (!epoch_match) {
        CS_LOG_WARN("message type {} epoch not match", msg->Index());
        return;
    }

    // handle sync proposal msg
    if (msg->Index() == ConsensusMessageType::RequestProposalMessage) {
        auto request_data = msg->RequestProposalData();
        consensus_io_ctx_->post([this, sender = sender, request_data]() {
            this->DoOnRecvRequestProposalMessage(sender, request_data);
        });
        return;
    }
    if (msg->Index() == ConsensusMessageType::ResponseProposalMessage) {
        auto response_data = msg->ResponseProposalData();
        consensus_io_ctx_->post([this, sender = sender, response_data]() {
            this->DoOnRecvResponseProposalMessage(sender, response_data);
        });
        return;
    }

    // handle receipt msg
    if ((msg->Index() >= ConsensusMessageType::ValReceiptMessage
         && msg->Index() <= ConsensusMessageType::AggregatedMainVoteReceiptMessage)
        || msg->Index() == ConsensusMessageType::ForwardSkipReceiptMessage) {
        reliable_channel_->OnRecvMessage(sender, msg);
        return;
    }
    // handle proof after epoch end
    if (is_epoch_end_.load() && msg->Index() == ConsensusMessageType::SkipMessage) {
        auto skip_data = msg->SkipData();
        consensus_io_ctx_->post([this, sender = sender, skip_data]() {
            this->DoOnRecvSkipMessage(sender, skip_data);
        });
        return;
    }

    // outdated msg: V7+ proof is in Pass/Skip only
    bool contain_proof = (msg->Index() == ConsensusMessageType::PassMessage
                          || msg->Index() == ConsensusMessageType::SkipMessage);

    if (info.seq_ < GetCurrSeq()) {
        reliable_channel_->SendReceipt(sender, msg);
        // should handle proof in val, skip, forward skip and pass
        // the special skip with seq == 0 is also handled
        if (info.seq_ % 10 == 0
            && (msg->Index() == ConsensusMessageType::SkipMessage
                || msg->Index() == ConsensusMessageType::ValMessage)) {
            Guard lock(inactive_proposers_lock_);
            if (inactive_proposers_.erase(sender) > 0) {
                CS_LOG_INFO("proposer {} is active again", toHex(sender).substr(0, 8));
            }
        }
        if (!contain_proof || (info.seq_ > 0 && info.seq_ <= last_stable_seq_.load())) {
            return;
        }
    }

    if (is_epoch_end_.load() && !contain_proof) {
        CS_LOG_INFO("epoch is end, not processing consensus messages");
        return;
    }
    // high water level = window * 2
    if (info.seq_ > (GetCurrSeq() + 2 * cfg_.consensus_window_)) {
        CS_LOG_ERROR("msg out of high water level {} last consensus {} last stable {}",
                     info.seq_,
                     last_consensus_seq_.load(),
                     last_stable_seq_.load());
        return;
    }

    // cannot send receipt
    // HandlePendingMessage guarantees no deadlock when msg.seq_ == GetCurrSeq()
    // the latter case is when window is full so cannot advance the pipeline
    if (info.seq_ > GetCurrSeq() || !CanPipeline(info.seq_)) {
        RecursiveGuard lock(pending_msg_lock_);
        PersistentKey key(sender);
        key.msg_info_ = info;
        pending_msg_.emplace(key, msg);
        return;
    }

    switch (msg->Index()) {
        case ConsensusMessageType::ValMessage: {
            auto val_data = msg->ValData();
            consensus_io_ctx_->post([this, sender = sender, val_data]() {
                this->DoOnRecvValMessage(sender, val_data, CheckSanityType::RECEIVED_FROM_OTHERS);
            });
            break;
        }
        case ConsensusMessageType::PassMessage: {
            auto pass_data = msg->PassData();
            consensus_io_ctx_->post([this, sender = sender, pass_data]() {
                this->DoOnRecvPassMessage(sender, pass_data);
            });
            break;
        }
        case ConsensusMessageType::SkipMessage: {
            auto skip_data = msg->SkipData();
            consensus_io_ctx_->post([this, sender = sender, skip_data]() {
                this->DoOnRecvSkipMessage(sender, skip_data);
            });
            break;
        }
        case ConsensusMessageType::ForwardSkipMessage: {
            auto forward_skip_data = msg->ForwardSkipData();
            consensus_io_ctx_->post([this, sender = sender, forward_skip_data]() {
                this->DoOnRecvForwardSkipMessage(sender, forward_skip_data);
            });
            break;
        }
        default: {
            auto myba = GetMyBA(info.proposer_id_);
            if (myba == nullptr) {
                CS_LOG_ERROR("invalid proposer in msg from {}", toHex(sender).substr(0, 8));
                return;
            }
            myba->PushMessage(sender, msg);
        }
    }
}

std::pair<MyTumblerProposeState, Seq> MyTumblerEngineBase::CanPropose() {
    if (stopped_ || is_epoch_end_.load()) {
        return {MyTumblerProposeState::PROPOSE_FAILED_EPOCH_END, 0};
    }
    // Acquire on is_proposed_ synchronizes with release stores in SetProposeState /
    // AdvanceProposeState, guaranteeing that subsequent relaxed loads of propose_seq_
    // and last_failed_seq_ see values at least as recent as the is_proposed_ store.
    if (propose_state_.is_proposed_.load(std::memory_order_acquire)) {
        return {MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS, 0};
    }
    // not checking is_proposer_, because next seq may change proposers

    if (!CanPipeline(propose_state_.propose_seq_.load(std::memory_order_relaxed))) {
        return {MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL, 0};
    }
    return {MyTumblerProposeState::PROPOSE_READY,
            propose_state_.last_failed_seq_.load(std::memory_order_relaxed)};
}

void MyTumblerEngineBase::Propose(bytes proposal,
                                  bool is_empty,
                                  const Timestamp timestamp,
                                  std::promise<std::pair<MyTumblerProposeState, Seq>>* res_prom) {
    if (!res_prom) {
        CS_LOG_ERROR("res_prom is nullptr");
        return;
    }

    // async, do not care val
    consensus_io_ctx_->post(
        [this, proposal = std::move(proposal), is_empty, timestamp, res_prom]() {
            DoPropose(std::move(proposal), is_empty, timestamp, res_prom);
        });
}

void MyTumblerEngineBase::DoPropose(bytes proposal,
                                    bool is_empty,
                                    const Timestamp timestamp,
                                    std::promise<std::pair<MyTumblerProposeState, Seq>>* prom) {
    Seq curr_seq = propose_state_.propose_seq_.load(std::memory_order_relaxed);
    bool is_proposer = is_proposer_.load();
    if (GetCurrSeq() != curr_seq) {
        // next propose seq may change proposers
        if (auto itr = next_proposers_.find(curr_seq); itr != next_proposers_.end()) {
            is_proposer = (itr->second.count(my_id_) > 0);
        }
    }
    if (!is_proposer) {
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_FAILED_NOT_PROPOSER, 0));
        return;
    }

    if (!CanPipeline(curr_seq)) {
        CS_LOG_DEBUG("pipeline window is full");
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL, 0));
        return;
    }

    if (is_epoch_end_.load()) {
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_FAILED_EPOCH_END, 0));
        return;
    }

    if (propose_state_.is_proposed_.load(std::memory_order_relaxed)) {
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS, 0));
        return;
    }

    // self or others have passed this seq, meaningless to propose
    if (curr_seq == GetCurrSeq()
        && (passed_.find(my_id_) != passed_.end() || passed_balance_ >= quorum_balance_)) {
        CS_LOG_WARN("{} has been passed, passed size: {}, balance {}",
                    curr_seq,
                    passed_.size(),
                    passed_balance_);
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS, 0));
        return;
    }
    // if (skipped_.find(my_id_) != skipped_.end()) {
    //     CS_LOG_WARN("%" PRIu64 " has been skipped", curr_seq);
    //     prom->set_value(MyTumblerProposeState::PROPOSE_FAILED_REFUSE);
    //     return;
    // }

    if (is_empty && (curr_seq != GetCurrSeq() || GetLeader(propose_empty_turn_) != my_id_)) {
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_FAILED_NOT_ALLOW_EMPTY, 0));
        return;
    }
    // accept proposal
    if (curr_seq == GetCurrSeq()) {
        // stop timer, no skip, no empty proposal
        skip_send_timer_.Stop();
    }

    // in_consensus_.store(true);
    propose_state_.is_proposed_.store(true, std::memory_order_release);

    if (prom) {
        prom->set_value(std::make_pair(MyTumblerProposeState::PROPOSE_SUCCESS, curr_seq));
    }

    ValMessagePtr val =
        CreateValMessage(curr_seq, my_id_, timestamp, std::move(proposal), epoch_number_);

    Signature tmp_sig;
    crypto_helper_->AggregateSign(CalculateBvalDigestForValMessage(val, spec_version_), tmp_sig);
    val->signature = std::move(tmp_sig);

    if (check_validity_) {
        if (!CheckMessageSanity(curr_seq, my_id_, CheckSanityType::SELF_PROPOSED, val)) {
            CS_LOG_ERROR("check sanity fail");
            return;
        }
        CS_LOG_DEBUG("check external validity successfully");
    }
}

void MyTumblerEngineBase::UpdateSyncRawSeq(const Seq seq,
                                           const Timestamp ts,
                                           const bool is_epoch_end) {
    consensus_io_ctx_->post([this, seq, ts, is_epoch_end]() {
        this->DoUpdateRawSeq(seq, ts, true, is_epoch_end);
    });
}

void MyTumblerEngineBase::UpdateStableSeq(const Seq seq, std::set<NodeId> next_proposers) {
    consensus_io_ctx_->post([this, seq, next_proposers = std::move(next_proposers)]() {
        this->DoUpdateStableSeq(seq, std::move(next_proposers));
    });
}

void MyTumblerEngineBase::UpdateFinishedSequence(const Seq seq,
                                                 std::vector<std::string> block_checkpoints) {
    consensus_io_ctx_->post([this, seq, block_checkpoints = std::move(block_checkpoints)]() {
        this->DoUpdateFinishedSequence(seq, std::move(block_checkpoints));
    });
}

void MyTumblerEngineBase::DoOnRecvValMessage(const NodeId& sender,
                                             const ValMessagePtr msg,
                                             const CheckSanityType type) {
    if (msg == nullptr) {
        return;
    }

    // network layer verifies sender's authenticity, so no need to verify signature here
    NodeId nodeid_str = NodeId(msg->proposer_id.Acquire());
    if (sender != nodeid_str) {
        CS_LOG_ERROR("receive val from fake proposer, sender: {}, proposer: {}",
                     toHex(sender).substr(0, 8),
                     toHex(nodeid_str).substr(0, 8));
        return;
    }
    if (!current_proposers_.count(nodeid_str)) {
        CS_LOG_ERROR("receive val from wrong proposer: {}", toHex(nodeid_str).substr(0, 8));
        return;
    }
    Digest hash_str = Digest(msg->hash.Acquire());
    if (val_.count(msg->seq) && val_[msg->seq].count(nodeid_str)
        && !val_[msg->seq][nodeid_str].empty()) {
        if (val_[msg->seq][nodeid_str].count(hash_str)) {
            reliable_channel_->SendReceipt(sender,
                                           std::make_shared<ssz_types::ConsensusMessage>(msg));
            return;
        } else {
            CS_LOG_ERROR("duplicated val msg from {}, this hash {}, previous hash {}",
                         toHex(sender).substr(0, 8),
                         toHex(hash_str).substr(0, 8),
                         toHex(val_[msg->seq][nodeid_str].begin()->first).substr(0, 8));
            return;
        }
    }
    if (unchecked_proposals_.count(msg->seq) && unchecked_proposals_[msg->seq].count(sender)) {
        for (const auto& [check_type, proposal] : unchecked_proposals_[msg->seq][sender]) {
            if (check_type == CheckSanityType::RECEIVED_FROM_OTHERS
                || check_type == CheckSanityType::RECOVERED) {
                CS_LOG_ERROR("duplicated val msg from {} under check sanity, this hash {}",
                             toHex(sender).substr(0, 8),
                             toHex(hash_str).substr(0, 8));
                return;
            }
        }
    }

    if (msg->seq <= last_consensus_seq_.load() || is_epoch_end_.load()) {
        return;
    }

    if (spec_version_ < CONSENSUS_VERSION_NO_TIMESTAMP_CHECK
        && msg->timestamp <= last_consensus_ts_.load()) {
        // must come from byzantine proposer, do nothing
        // from V9, we have pipeline propose so no this check
        CS_LOG_ERROR("proposal timestamp {} , last block ts {}",
                     msg->timestamp,
                     last_consensus_ts_.load());
        return;
    }

    if (CalculateValMessageHash(msg) != Digest(msg->hash.Acquire())) {
        CS_LOG_ERROR("invalid hash in val msg from {}", msg->proposer_id.Hex().substr(0, 8));
        return;
    }

    CS_LOG_INFO("receive val from {}, seq: {} hash: {}",
                toHex(sender).substr(0, 8),
                msg->seq,
                msg->hash.Hex().substr(0, 8));
    {
        Guard lock(inactive_proposers_lock_);
        inactive_proposers_.erase(sender);
        silent_this_round_.erase(sender);
    }

    if (!CheckMessageSanity(msg->seq, sender, type, msg)) {
        CS_LOG_ERROR("check sanity fail");
        return;
    }
}

void MyTumblerEngineBase::DoOnRecvPassMessage(const NodeId& sender, const PassMessagePtr msg) {
    if (msg == nullptr) {
        return;
    }

    if (sender != my_id_) {
        // handle proofs from Pass (V7+)
        std::vector<BlockProofBuffer> stable_proofs;
        for (const auto& proof_view : msg->encoded_stable_proof.Acquire()) {
            stable_proofs.emplace_back(proof_view.Acquire());
        }
        std::vector<BlockProofBuffer> checkpoints;
        for (const auto& checkpoint_view : msg->encoded_block_checkpoint_info.Acquire()) {
            checkpoints.emplace_back(checkpoint_view.Acquire());
        }
        handle_block_proof_cb_(std::move(stable_proofs), std::move(checkpoints));
    }

    if (msg->seq <= last_consensus_seq_.load() || is_epoch_end_.load()) {
        CS_LOG_INFO("receive pass from {}, seq: {}, contains {} proofs, outdated",
                    toHex(sender).substr(0, 8),
                    msg->seq,
                    msg->encoded_block_checkpoint_info.Size());
        return;
    }

    if (passed_.try_emplace(sender, msg).second) {
        CS_LOG_INFO("receive pass from {}, seq: {}, contains {} proofs",
                    toHex(sender).substr(0, 8),
                    msg->seq,
                    msg->encoded_block_checkpoint_info.Size());
        passed_balance_ += (*peers_balance_)[sender];
    } else {
        CS_LOG_INFO("receive pass from {}, seq: {}, contains {} proofs, duplicated",
                    toHex(sender).substr(0, 8),
                    msg->seq,
                    msg->encoded_block_checkpoint_info.Size());
        return;
    }

    if (sender != my_id_) {
        for (auto& proposer : msg->endorsed.Acquire()) {
            if (peers_->find(NodeId(proposer.Acquire())) == peers_->end()) {
                CS_LOG_ERROR("invalid proposer in endorsed from {}", toHex(sender).substr(0, 8));
                continue;
            }

            if (current_proposers_.find(NodeId(proposer.Acquire())) == current_proposers_.end()) {
                CS_LOG_ERROR("not current proposer in endorsed from {}",
                             toHex(sender).substr(0, 8));
                continue;
            }
            // when consensus=1 is added to ba_complete_,
            // but val not yet received, it was already added to pending_
            if (ba_complete_.find(NodeId(proposer.Acquire())) != ba_complete_.end()) {
                continue;
            }
            pending_[msg->seq].emplace(NodeId(proposer.Acquire()));
        }
    }

    CheckPassStatus();
}

void MyTumblerEngineBase::DoOnRecvSkipMessage(const NodeId& sender, const SkipMessagePtr msg) {
    if (msg == nullptr) {
        return;
    }
    {
        // handle proofs
        std::vector<BlockProofBuffer> stable_proofs;
        for (const auto& proof_view : msg->encoded_stable_proof.Acquire()) {
            stable_proofs.emplace_back(proof_view.Acquire());
        }
        std::vector<BlockProofBuffer> checkpoints;
        for (const auto& checkpoint_view : msg->encoded_block_checkpoint_info.Acquire()) {
            checkpoints.emplace_back(checkpoint_view.Acquire());
        }
        handle_block_proof_cb_(std::move(stable_proofs), std::move(checkpoints));
    }

    // continue to handle SkipMessage
    if (msg->seq <= last_consensus_seq_.load() || is_epoch_end_.load()) {
        return;
    }

    NodeId nodeid_str = NodeId(msg->proposer_id.Acquire());

    if (!current_proposers_.count(nodeid_str)) {
        CS_LOG_ERROR("not current proposer in skip from {}", toHex(nodeid_str).substr(0, 8));
        return;
    }

    if (skipped_.find(nodeid_str) != skipped_.end()) {
        reliable_channel_->SendReceipt(sender, std::make_shared<ssz_types::ConsensusMessage>(msg));
        return;
    }

    if (nodeid_str != my_id_) {
        if (!crypto_helper_->VerifySignature(CalculateSkipDigest(msg),
                                             Signature(msg->signature.Acquire()),
                                             nodeid_str,
                                             peers_)) {
            CS_LOG_ERROR("verify skip signature failed of {} from {}",
                         msg->proposer_id.Hex().substr(0, 8),
                         toHex(sender).substr(0, 8));
            return;
        }
    }

    reliable_channel_->OnRecvMessage(sender, std::make_shared<ssz_types::ConsensusMessage>(msg));
    CS_LOG_INFO("receive skip from {}, seq: {}", msg->proposer_id.Hex().substr(0, 8), msg->seq);
    skipped_.emplace(nodeid_str, Signature(msg->signature.Acquire()));
    auto result = output_.insert(nodeid_str);
    if (result.second) {
        output_balance_ += peers_balance_->at(nodeid_str);
    }
    {
        Guard lock(inactive_proposers_lock_);
        inactive_proposers_.erase(nodeid_str);
        silent_this_round_.erase(nodeid_str);
    }

    CheckCanPass();
}

void MyTumblerEngineBase::DoOnRecvForwardSkipMessage(const NodeId& sender,
                                                     const ForwardSkipMessagePtr msg) {
    if (msg == nullptr) {
        return;
    }
    // V7+ proof is in Pass, not in ForwardSkip

    // continue to handle ForwardSkipMessage
    if (msg->seq <= last_consensus_seq_.load() || is_epoch_end_.load()) {
        return;
    }

    // already pass, ignore ForwardSkipMessage
    if (passed_.find(my_id_) != passed_.end()) {
        return;
    }

    // verify signatures and add to skip map
    for (const auto& it : msg->skip_signatures.Acquire()) {
        const NodeId nodeid_str(it.node_id.Acquire());
        if (skipped_.find(nodeid_str) != skipped_.end()) {
            continue;
        }

        if (!current_proposers_.count(nodeid_str)) {
            CS_LOG_ERROR("not current proposer in skip from {}", toHex(nodeid_str).substr(0, 8));
            continue;
        }

        SkipMessagePtr skip_msg = CreateSkipMessage(msg->seq, nodeid_str, Signature());
        if (!crypto_helper_->VerifySignature(CalculateSkipDigest(skip_msg),
                                             Signature(it.signature.Acquire()),
                                             nodeid_str,
                                             peers_)) {
            CS_LOG_ERROR("verify skip signature failed of {} from {}",
                         toHex(nodeid_str).substr(0, 8),
                         toHex(sender).substr(0, 8));
            continue;
        }

        CS_LOG_INFO("receive skip of {} in ForwardSkipMsg, seq: {}",
                    toHex(nodeid_str).substr(0, 8),
                    msg->seq);
        skipped_.emplace(nodeid_str, Signature(it.signature.Acquire()));
        auto result = output_.insert(nodeid_str);
        if (result.second) {
            output_balance_ += peers_balance_->at(nodeid_str);
        }

        // persist skip msg
        skip_msg->signature = Signature(it.signature.Acquire());
        reliable_channel_->SaveOutgoingMessage(
            std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
    }

    // send receipt
    // forward skip msg itself no need persist
    reliable_channel_->SendReceipt(sender, std::make_shared<ssz_types::ConsensusMessage>(msg));

    CheckCanPass();
}

void MyTumblerEngineBase::DoUpdateRawSeq(const Seq seq,
                                         const Timestamp timestamp,
                                         bool is_sync,
                                         const bool is_epoch_end) {
    if (is_epoch_end && !is_epoch_end_.load()) {
        is_epoch_end_.store(is_epoch_end);
        resend_proof_timer_.ResetTimeoutInterval(cfg_.broadcast_proof_interval_);
        resend_proof_timer_.Reset();
        CS_LOG_INFO("epoch end reset resend proof timer");
    }

    auto last_consensus_seq = last_consensus_seq_.load();
    if (seq <= last_consensus_seq) {
        CS_LOG_DEBUG("old raw block({}), last_exec({}), maybe just updated during sync",
                     seq,
                     last_consensus_seq);
        return;
    }
    last_consensus_ts_.store(timestamp);
    last_consensus_seq_.store(seq);
    propose_empty_turn_ = seq + 1;
    if (is_sync) {
        CS_LOG_DEBUG("sync raw seq: {} last consensus: {}", seq, last_consensus_seq);
        CETINA_COUNTER_INC(MYTUMBLER_SYNC_BLOCK_GAUGE);
        ++sync_completed_count_;
    }
    delay_pass_ = true;
    reliable_channel_->ResetResendTimer();
    CS_LOG_INFO("UpdateRawBlock {} ok, is epoch end {}", seq, is_epoch_end);
    last_rawblock_finish_time_ = time_utils::GetSteadyTimePoint();
    auto kv_handle = next_proposers_.extract(seq + 1);
    if (!kv_handle.empty()) {
        current_proposers_ = std::move(kv_handle.mapped());
        is_proposer_.store(current_proposers_.count(my_id_));
        CS_LOG_INFO("Update proposers, I am proposer {}", is_proposer_.load());
    }
    if (is_sync) {
        ClearCurrSeqState(seq);
        if (!is_epoch_end) {
            // in_consensus_.store(false);
            if (propose_state_.propose_seq_.load(std::memory_order_relaxed) < seq + 1) {
                // I don't know this seq proposal is successful or not, so pessimistically set last
                // failed seq
                // must first set last_failed_seq_ then is_proposed_, as CanPropose check in the
                // reverse order
                if (propose_state_.is_proposed_.load(std::memory_order_relaxed)) {
                    propose_state_.last_failed_seq_.store(
                        propose_state_.propose_seq_.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
                }
                propose_state_.AdvanceProposeState(false, seq + 1);
            }
            HandlePendingMessage(seq + 1);
        }
    }
}

void MyTumblerEngineBase::DoUpdateFinishedSequence(
    // only called when version > 0
    const Seq seq,
    std::vector<BlockProofBuffer> block_checkpoints) {
    CS_LOG_INFO("UpdateFinishedSequence {} ok, number of checkpoints {}",
                seq,
                block_checkpoints.size());
    last_finished_seq_ = seq;
    my_finished_proofs_[seq] = std::move(block_checkpoints);
    Seq last_consensus = last_consensus_seq_.load();
    if (seq + cfg_.consensus_window_
        <= last_consensus + 1 + 1) {  // window was full, now can proceed
        CETINA_GAUGE_SET(MYTUMBLER_WINDOW_FULL, 1);
        CheckCanPass();
        if (!is_epoch_end_.load()) {
            HandlePendingMessage(last_consensus + 1);
        }
    } else if (CanPipeline(last_consensus + 1)) {
        // seq + consensus_window_ > last_consensus + 2, window clear
        CETINA_GAUGE_SET(MYTUMBLER_WINDOW_FULL, 0);
    }
}

void MyTumblerEngineBase::DoUpdateStableSeq(const Seq seq, std::set<NodeId> next_proposers) {
    Seq last_stable = last_stable_seq_.load();
    Seq last_consensus = last_consensus_seq_.load();
    CS_LOG_INFO("update stable seq {}, last consensus {}, last stable {}",
                seq,
                last_consensus,
                last_stable);
    if (seq != last_stable + 1) {
        CS_LOG_FATAL("stable block jump from ({}) to ({})", last_stable, seq);
        COBRE_ABORT();
    }
    if (seq > last_consensus) {
        CS_LOG_WARN("stable block number {} last consensus {}", seq, last_consensus);
    }
    if (seq > last_finished_seq_) {
        CS_LOG_WARN("stable block number {} overwrites last finished {}", seq, last_finished_seq_);
        last_finished_seq_ = seq;
    }

    CETINA_GAUGE_SET(MYTUMBLER_WINDOW_INDEX, (last_consensus + 1 - seq));
    last_stable_seq_.store(seq);
    last_stable_update_time_ = time_utils::GetCurrentTimestamp();
    GarbageCollection(seq);

    if (!next_proposers.empty()) {
        next_proposers_[seq + cfg_.proposer_shuffle_window_] = std::move(next_proposers);
        next_proposers_.erase(next_proposers_.begin(), next_proposers_.upper_bound(seq));
    }

    CS_LOG_INFO("AppendStableBlock {} ok", seq);

    if (seq + cfg_.consensus_window_ <= last_consensus + 1) {  // window was full, now can proceed
        CETINA_GAUGE_SET(MYTUMBLER_WINDOW_FULL, 1);
        CheckCanPass();
        if (!is_epoch_end_.load()) {
            HandlePendingMessage(last_consensus + 1);
        }
    } else if (CanPipeline(last_consensus + 1)) {  // seq + consensus_window_ > last_consensus + 1
        CETINA_GAUGE_SET(MYTUMBLER_WINDOW_FULL, 0);
    }
}

void MyTumblerEngineBase::CheckSanityDone(const Seq seq_number,
                                          const NodeId& node_id,
                                          const std::pair<uint32_t, uint32_t> index_epoch,
                                          std::optional<bytes> new_proposal) {
    consensus_io_ctx_->post(
        [this, seq_number, node_id, index_epoch, new_proposal = std::move(new_proposal)]() mutable {
            this->DoOnCheckSanityDone(seq_number, node_id, index_epoch, std::move(new_proposal));
        });
}

void MyTumblerEngineBase::DoOnCheckSanityDone(const Seq seq_number,
                                              const NodeId& node_id,
                                              const std::pair<uint32_t, uint32_t> index_epoch,
                                              std::optional<bytes> new_proposal) {
    CS_LOG_INFO("check message sanity finished, seq: {} proposer: {}",
                seq_number,
                toHex(node_id).substr(0, 8));
    uint32_t index = index_epoch.first;
    uint32_t epoch = index_epoch.second;
    if (epoch != epoch_number_) {
        CS_LOG_WARN("mismatch epoch check sanity for seq {} node_id {}",
                    seq_number,
                    toHex(node_id).substr(0, 8));
        return;
    }
    if (seq_number < GetCurrSeq()) {
        CS_LOG_WARN("outdated check sanity for seq {} node_id {}",
                    seq_number,
                    toHex(node_id).substr(0, 8));
        return;
    }
    if (unchecked_proposals_.find(seq_number) != unchecked_proposals_.end()
        && unchecked_proposals_[seq_number].find(node_id) != unchecked_proposals_[seq_number].end()
        && unchecked_proposals_[seq_number][node_id].size() >= index) {
        auto type = unchecked_proposals_[seq_number][node_id][index - 1].first;
        auto val = unchecked_proposals_[seq_number][node_id][index - 1].second;
        Digest hash_str = Digest(val->hash.Acquire());
        switch (type) {
            case CheckSanityType::SELF_PROPOSED: {
                if (seq_number == GetCurrSeq()) {
                    if (passed_.find(my_id_) != passed_.end()
                        || passed_balance_ >= quorum_balance_) {
                        CS_LOG_WARN("{} has been passed, passed size: {}, balance {}",
                                    seq_number,
                                    passed_.size(),
                                    passed_balance_);
                        return;
                    }
                    if (skipped_.find(my_id_) != skipped_.end()) {
                        CS_LOG_WARN("{} has been skipped", seq_number);
                        return;
                    }
                }
                if (new_proposal) {
                    // persister modified proposal
                    val->payload = std::move(new_proposal.value());
                    val->hash = CalculateValMessageHash(val);
                    hash_str = Digest(val->hash.Acquire());
                    Signature tmp_sig;
                    crypto_helper_->AggregateSign(
                        CalculateBvalDigestForValMessage(val, spec_version_),
                        tmp_sig);
                    val->signature = std::move(tmp_sig);
                    CS_LOG_INFO("changed proposal for seq: {}", val->seq);
                }
                ReliableBroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(val));

                if (val_[seq_number].empty()) {
                    UpdateConsensusStartTime(seq_number);
                }

                val_[seq_number][node_id][hash_str] = val;
                endorsed_[seq_number].emplace(node_id);
                pending_[seq_number].emplace(node_id);

                auto bval_msg = CreateBvalMessage(val->seq,
                                                  NodeId(val->proposer_id.Acquire()),
                                                  0,
                                                  Digest(val->hash.Acquire()),
                                                  true,
                                                  epoch_number_);
                bval_msg->signature = Signature(val->signature.Acquire());
                auto consensus_message = std::make_shared<ssz_types::ConsensusMessage>(bval_msg);
                if (seq_number == GetCurrSeq()) {
                    auto myba = GetMyBA(node_id);
                    if (myba == nullptr) {
                        CS_LOG_ERROR("invalid proposer in val msg from {}",
                                     val->proposer_id.Hex().substr(0, 8));
                        return;
                    }
                    myba->PushMessage(node_id, consensus_message);
                } else if (seq_number > GetCurrSeq()) {
                    // propose in advance, should not push to BA
                    PersistentKey key(seq_number,
                                      node_id,
                                      node_id,
                                      ConsensusMessageType::BvalMessage,
                                      bval_msg->round,
                                      Digest(bval_msg->hash.Acquire()));
                    RecursiveGuard lock(pending_msg_lock_);
                    pending_msg_.emplace(key, consensus_message);
                }

                CS_LOG_INFO("Propose, seq: {} proposer: {} hash: {}",
                            val->seq,
                            toHex(node_id).substr(0, 8),
                            val->hash.Hex().substr(0, 8));
                break;
            }
            case CheckSanityType::RECEIVED_FROM_OTHERS:
            case CheckSanityType::RECOVERED: {
                // may have recovered seq + 2
                if (val_[val->seq].empty()) {
                    UpdateConsensusStartTime(val->seq);
                }
                if (val_[val->seq][node_id].size() > 0) {
                    CS_LOG_WARN("duplicated val msg from {}", toHex(node_id).substr(0, 8));
                    return;
                }
                val_[val->seq][node_id][hash_str] = val;
                if (type == CheckSanityType::RECEIVED_FROM_OTHERS) {
                    reliable_channel_->OnRecvMessage(
                        node_id,
                        std::make_shared<ssz_types::ConsensusMessage>(val));
                } else {
                    // recovered no need to save again
                    reliable_channel_->SendReceipt(
                        node_id,
                        std::make_shared<ssz_types::ConsensusMessage>(val));
                }
                if (val->seq == GetCurrSeq()) {
                    if (ba_success_.find(node_id) != ba_success_.end()
                        && ba_success_[node_id] == hash_str) {
                        pending_[val->seq].erase(node_id);
                        CheckConsensusComplete();
                        return;
                    }
                    if (ba_complete_.find(node_id) != ba_complete_.end()) {
                        return;
                    }
                }

                auto bval_msg = CreateBvalMessage(val->seq,
                                                  NodeId(val->proposer_id.Acquire()),
                                                  0,
                                                  Digest(val->hash.Acquire()),
                                                  true,
                                                  epoch_number_);
                bval_msg->signature = Signature(val->signature.Acquire());
                auto consensus_message = std::make_shared<ssz_types::ConsensusMessage>(bval_msg);
                if (val->seq == GetCurrSeq()) {
                    auto myba = GetMyBA(node_id);
                    if (myba == nullptr) {
                        CS_LOG_ERROR("invalid proposer in val msg from {}",
                                     toHex(node_id).substr(0, 8));
                        return;
                    }
                    myba->PushMessage(node_id,
                                      std::make_shared<ssz_types::ConsensusMessage>(bval_msg));

                    if (passed_.find(my_id_) != passed_.end()) {
                        // if already passed, must VoteZero regardless of whether it's a fresh
                        // restart BA will further check conditions; val messages are processed last
                        // on restart to ensure BA recovers state first
                        myba->SwitchToNormalPath(val->seq, node_id);
                        already_vote_zero_.insert(node_id);

                        // after restart, continue to Endorse previously endorsed content
                        if (endorsed_.count(val->seq) && endorsed_[val->seq].count(node_id)
                            && val_[val->seq][node_id].size() == 1) {
                            myba->Endorse(val->seq, node_id, hash_str, true);
                        }
                    } else {
                        myba->Endorse(val->seq, node_id, hash_str, true);
                        endorsed_[val->seq].emplace(node_id);
                    }
                    // if a quorum has PASSed, then this is either saved or to be abandoned, no need
                    // to pend
                    if (passed_balance_ < quorum_balance_) {
                        pending_[val->seq].insert(node_id);
                    }
                } else if (val->seq > GetCurrSeq()
                           && NodeId(val->proposer_id.Acquire()) == my_id_) {
                    // from next seq, current version must be self proposed
                    endorsed_[val->seq].emplace(node_id);
                    pending_[val->seq].insert(node_id);
                    PersistentKey key(val->seq,
                                      my_id_,
                                      my_id_,
                                      ConsensusMessageType::BvalMessage,
                                      bval_msg->round,
                                      Digest(bval_msg->hash.Acquire()));
                    RecursiveGuard lock(pending_msg_lock_);
                    pending_msg_.emplace(key, consensus_message);
                }
                break;
            }
            case CheckSanityType::PULLED_FROM_OTHERS: {
                val_[seq_number][node_id][hash_str] = val;
                pending_[seq_number].erase(NodeId(val->proposer_id.Acquire()));
                reliable_channel_->OnRecvMessage(
                    node_id,
                    std::make_shared<ssz_types::ConsensusMessage>(val));
                CheckConsensusComplete();
                break;
            }
            default:
                CS_LOG_ERROR("unreachable");
                break;
        }
    } else {
        CS_LOG_ERROR("wrong CheckSanityDone call for seq {} node_id {}",
                     seq_number,
                     toHex(node_id).substr(0, 8));
    }
}

void MyTumblerEngineBase::RecordVoterCallBack(const Seq seq_number,
                                              const NodeId& proposer,
                                              const Digest& digest,
                                              const NodeId& voter) {
    consensus_io_ctx_->post([this, seq_number, proposer, digest, voter]() {
        this->DoRecordVoter(seq_number, proposer, digest, voter);
    });
}

void MyTumblerEngineBase::DoRecordVoter(const Seq seq_number,
                                        const NodeId& proposer,
                                        const Digest& digest,
                                        const NodeId& voter) {
    if (seq_number != GetCurrSeq()) {
        return;
    }
    voters_with_tx_[proposer][digest].emplace_back(voter);
}

bool MyTumblerEngineBase::CheckMessageSanity(Seq seq_number,
                                             const NodeId& propose_node,
                                             CheckSanityType type,
                                             const ValMessagePtr proposal) {
    CS_LOG_DEBUG("check message sanity seq {}, propose node {}",
                 seq_number,
                 toHex(propose_node).substr(0, 8));
    if (proposal == nullptr) {
        CS_LOG_ERROR("proposal is nullptr");
        return false;
    }
    if (unchecked_proposals_.count(seq_number)
        && unchecked_proposals_[seq_number].count(propose_node)) {
        for (const auto& existing : unchecked_proposals_[seq_number][propose_node]) {
            if (existing.first == type
                && existing.second->hash.Acquire() == proposal->hash.Acquire()) {
                return true;
            }
        }
    }
    unchecked_proposals_[seq_number][propose_node].emplace_back(std::make_pair(type, proposal));
    auto sv = proposal->payload.Acquire();
    auto payload = std::make_shared<ABuffer>(ABuffer::DeepCopy(
        reinterpret_cast<const uint8_t*>(sv.data()), sv.size()));
    return check_validity_(seq_number,
                           propose_node,
                           std::make_pair(unchecked_proposals_[seq_number][propose_node].size(),
                                          epoch_number_),  // as ref when callback
                           payload);
}

// when recv f+1 bval, myba thread call this to endorse, prevent consensus thread has passed before
void MyTumblerEngineBase::EndorseCallBack(const Seq seq,
                                          const NodeId& proposer,
                                          const Digest& hash) {
    consensus_io_ctx_->post([this, seq, proposer, hash]() {
        this->DoEndorseCallBack(seq, proposer, hash);
    });
}

void MyTumblerEngineBase::DoEndorseCallBack(const Seq seq,
                                            const NodeId& proposer,
                                            const Digest& hash) {
    if (is_epoch_end_.load()) {
        // prevent very faulty case
        return;
    }
    if (seq == GetCurrSeq()) {
        auto myba = GetMyBA(proposer);
        if (passed_.find(my_id_) != passed_.end()) {
            myba->SwitchToNormalPath(seq, proposer);
            already_vote_zero_.insert(proposer);
        } else {
            bool val_exist = false;
            if (val_.find(seq) != val_.end() && val_[seq].find(proposer) != val_[seq].end()
                && val_[seq][proposer].find(hash) != val_[seq][proposer].end()) {
                val_exist = true;
            }

            myba->Endorse(seq, proposer, hash, val_exist);
            endorsed_[seq].emplace(proposer);
        }
    }
}

void MyTumblerEngineBase::MyBACompleteCallBack(const Seq seq,
                                               const NodeId& proposer,
                                               const Digest& hash) {
    consensus_io_ctx_->post([this, seq, proposer, hash]() {
        this->DoMyBACompleteCallBack(seq, proposer, hash);
    });
}

void MyTumblerEngineBase::DoMyBACompleteCallBack(const Seq seq,
                                                 const NodeId& proposer,
                                                 const Digest& hash) {
    if (seq <= last_consensus_seq_.load()) {
        CS_LOG_DEBUG("old myba callback, seq: {} last_consensus: {}",
                     seq,
                     last_consensus_seq_.load());
        return;
    } else if (seq > GetCurrSeq()) {
        CS_LOG_ERROR("should NOT happen: future myba finished seq: {} proposer: {}",
                     seq,
                     toHex(proposer).substr(0, 8));
        return;
    }

    if (ba_complete_.find(proposer) != ba_complete_.end()) {
        CS_LOG_INFO("duplicate myba finished seq: {} proposer: {} decide: {}",
                    seq,
                    toHex(proposer).substr(0, 8),
                    toHex(hash).substr(0, 8));
        return;
    }

    CS_LOG_INFO("myba finished seq: {} proposer: {} decide: {}",
                seq,
                toHex(proposer).substr(0, 8),
                toHex(hash).substr(0, 8));

    ba_complete_.insert(proposer);
    if (hash != ZERO_32_BYTES) {
        ba_success_[proposer] = hash;
        auto result = output_.insert(proposer);
        if (result.second) {
            output_balance_ += peers_balance_->at(proposer);
        }

        if (ba_success_.size() == 1) {
            CheckCanSkip();
            soft_pass_timer_.Reset();
        }

        if (proposer == my_id_) {
            // must first set last_failed_seq_ then is_proposed_, as CanPropose check in the
            // reverse order
            propose_state_.last_failed_seq_.store(0, std::memory_order_relaxed);
            // only advance if passed
            if (passed_.find(my_id_) != passed_.end()) {
                propose_state_.AdvanceProposeState(false, seq + 1);
            }
        }

        if (val_[seq].find(proposer) == val_[seq].end()
            || val_[seq][proposer].find(hash) == val_[seq][proposer].end()) {
            pending_[seq].insert(proposer);
            CheckCanPass();
            return;
        }
    } else if (proposer == my_id_) {
        // must first set last_failed_seq_ then is_proposed_, as CanPropose check in the reverse
        // order
        propose_state_.last_failed_seq_.store(seq, std::memory_order_relaxed);
        // only advance if passed
        if (passed_.find(my_id_) != passed_.end()) {
            propose_state_.AdvanceProposeState(false, seq + 1);
        }
        CETINA_COUNTER_INC(MYBA_ZERO_COUNT);
        ++proposal_rejected_count_;
    }

    pending_[seq].erase(proposer);
    CheckCanPass();
    CheckConsensusComplete();
}

bool MyTumblerEngineBase::CanPipeline(const Seq num) {
    return num <= last_stable_seq_.load() + cfg_.consensus_window_
           && num + 1 <= last_finished_seq_ + cfg_.consensus_window_;
}

// check if curr seq has finished
// called when receive enough pass or some myba finished(myba callback or receive val)
void MyTumblerEngineBase::CheckConsensusComplete() {
    if (passed_balance_ >= quorum_balance_) {
        if (pending_[GetCurrSeq()].empty()) {
            DoOnConsensusComplete(GetCurrSeq());
        } else {
            std::string pending_nodes;
            for (const auto& node : pending_[GetCurrSeq()]) {
                pending_nodes += toHex(node).substr(0, 8) + " ";
            }
            CS_LOG_INFO("passed size {}, passed balance {}, pending nodes {}",
                        passed_.size(),
                        passed_balance_,
                        pending_nodes);
        }
    }
}

void MyTumblerEngineBase::ClearCurrSeqState(const Seq seq) {
    // val_.clear();
    pending_.erase(pending_.begin(), pending_.upper_bound(seq));
    already_vote_zero_.clear();
    ba_complete_.clear();
    ba_success_.clear();
    endorsed_.erase(endorsed_.begin(), endorsed_.upper_bound(seq));
    skipped_.clear();
    passed_.clear();
    passed_balance_ = 0;
    output_.clear();
    output_balance_ = 0;
    silent_this_round_ = current_proposers_;
    voters_with_tx_.clear();
    forward_skip_timer_.Stop();
    can_skip_ = false;
    bool is_next_seq_proposed =
        propose_state_.propose_seq_.load(std::memory_order_relaxed) == seq + 1
        && propose_state_.is_proposed_.load(std::memory_order_relaxed);
    if (!is_epoch_end_.load() && !is_next_seq_proposed) {
        skip_send_timer_.Reset();
    }

    is_hard_pass_ = false;
    if (!is_epoch_end_.load()) {
        hard_pass_timer_.Reset();
    }
    soft_pass_timer_.Stop();
}

// check if can send pass
// called when receive skip or some myba finished with 1
void MyTumblerEngineBase::CheckCanPass() {
    if (passed_.find(my_id_) != passed_.end()) {
        return;
    }

    // V7+ only: all proposers have output or skip, or hard pass timeout
    if (output_.size() == current_proposers_.size()) {
        Pass();
        return;
    }
    // check if ba_complete + skipped + inactive_proposers_ contains all current_proposers_
    bool all_complete = true;
    {
        Guard lock(inactive_proposers_lock_);
        if (ba_complete_.size() + skipped_.size() + inactive_proposers_.size()
            >= current_proposers_.size()) {
            for (const auto& proposer : current_proposers_) {
                if (ba_complete_.find(proposer) == ba_complete_.end()
                    && skipped_.find(proposer) == skipped_.end()
                    && inactive_proposers_.find(proposer) == inactive_proposers_.end()) {
                    all_complete = false;
                    break;
                }
            }
        } else {
            all_complete = false;
        }
    }
    if (all_complete) {
        Pass();
        return;
    }

    // hard pass timeout
    if (is_hard_pass_ && CanPipeline(GetCurrSeq())) {
        Pass();
    }
}

// check if can send skip
// called when skip timeout or some myba finished with 1
void MyTumblerEngineBase::CheckCanSkip() {
    if (skipped_.find(my_id_) != skipped_.end()) {
        return;
    }
    // only this seq and not proposed
    if (!(propose_state_.propose_seq_.load(std::memory_order_relaxed) == GetCurrSeq()
          && !propose_state_.is_proposed_.load(std::memory_order_relaxed))) {
        return;
    }

    if (!is_proposer_.load()) {
        return;
    }

    if (can_skip_ && !ba_success_.empty()) {
        // send skip
        SkipMessagePtr skip_msg = CreateSkipMessage(GetCurrSeq(), my_id_, Signature());
        Signature tmp_sig;
        crypto_helper_->Sign(CalculateSkipDigest(skip_msg), tmp_sig);
        skip_msg->signature = tmp_sig;
        ReliableBroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
        // Do not advance propose_state_ here; advance only when we Pass.
        if (propose_state_.propose_seq_.load(std::memory_order_relaxed) == GetCurrSeq()) {
            propose_state_.is_proposed_.store(true, std::memory_order_release);
        }
        CS_LOG_INFO("send skip in seq {}", GetCurrSeq());
        CETINA_COUNTER_INC(MYTUMBLER_SKIP_COUNT);
        ++skip_stats_count_;
        DoOnRecvSkipMessage(my_id_, skip_msg);
    }
}

void MyTumblerEngineBase::Pass() {
    if (propose_state_.propose_seq_.load(std::memory_order_relaxed) == GetCurrSeq()) {
        // not yet proposed, or skipped, or myba completed
        if (!propose_state_.is_proposed_.load(std::memory_order_relaxed)
            || skipped_.find(my_id_) != skipped_.end()
            || ba_complete_.find(my_id_) != ba_complete_.end()) {
            propose_state_.AdvanceProposeState(false, GetCurrSeq() + 1);
        }
    }
    hard_pass_timer_.Stop();
    soft_pass_timer_.Stop();

    for (const auto& silent_proposer : silent_this_round_) {
        if (silent_proposer != my_id_) {  // I'm never inactive
            CS_LOG_WARN("proposer {} is marked inactive", toHex(silent_proposer).substr(0, 8));
            Guard lock(inactive_proposers_lock_);
            inactive_proposers_.insert(silent_proposer);
            ++peer_inactive_events_[silent_proposer];
        }
    }

    std::vector<ssz::ByteVector<32>> tmp_endorsed;
    for (auto& value : endorsed_[GetCurrSeq()]) {
        tmp_endorsed.emplace_back(value);
    }
    auto pass_msg = CreatePassMessage(GetCurrSeq(), std::move(tmp_endorsed));

    // V7+ proof in Pass
    {
        Seq init_stable = 0;
        if (last_stable_seq_.load() > cfg_.consensus_window_) {
            init_stable = last_stable_seq_.load() - cfg_.consensus_window_;
        }
        for (auto itr = stable_proofs_.lower_bound(init_stable); itr != stable_proofs_.end();
             ++itr) {
            pass_msg->encoded_stable_proof.PushBack(itr->second);
        }
        // make sure every proof are sent
        if (!my_finished_proofs_.empty()) {
            Seq lb = std::min(last_stable_seq_.load(), last_sent_proof_seq_);
            for (auto itr = my_finished_proofs_.upper_bound(lb); itr != my_finished_proofs_.end();
                 ++itr) {
                for (const auto& my_proof : itr->second) {
                    pass_msg->encoded_block_checkpoint_info.PushBack(my_proof);
                }
            }
            last_sent_proof_seq_ = my_finished_proofs_.rbegin()->first;
        }
    }

    ReliableBroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(pass_msg), true);
    CS_LOG_INFO("broadcast pass {}", GetCurrSeq());
    if (passed_balance_ < quorum_balance_) {
        forward_skip_timer_.Reset();
    }
    pass_start_time_ = time_utils::GetSteadyTimePoint();
    DoOnRecvPassMessage(my_id_, pass_msg);
}

void MyTumblerEngineBase::DoOnSkipSendTimeout(const asio::error_code& code, bool cancel) {
    if (code) {
        CS_LOG_DEBUG("skip send timer return code {}", code.value());
        return;
    }
    if (cancel) {
        CS_LOG_DEBUG("skip send timer has been cancelled");
        return;
    }

    if (skipped_.find(my_id_) != skipped_.end()) {
        CS_LOG_WARN("skip send timeout, but seq {} has already send skip", GetCurrSeq());
        return;
    }

    can_skip_ = true;
    CheckCanSkip();
}

void MyTumblerEngineBase::DoOnForwardSkipTimeout(const asio::error_code& code, bool cancel) {
    if (code) {
        CS_LOG_DEBUG("forward skip timer return code {}", code.value());
        return;
    }
    if (cancel) {
        CS_LOG_DEBUG("forward skip timer has been cancelled");
        return;
    }

    std::vector<ssz_types::Signature> tmp_signatures;
    for (const auto& skip : skipped_) {
        ssz_types::Signature sig;
        sig.node_id = skip.first;
        sig.signature = skip.second;
        tmp_signatures.emplace_back(std::move(sig));
    }
    auto forward_skip_msg = CreateForwardSkipMessage(GetCurrSeq(), std::move(tmp_signatures));
    ReliableBroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(forward_skip_msg), false);
    CS_LOG_WARN("broadcast forward skip msg of seq {} contains {} skips",
                GetCurrSeq(),
                skipped_.size());
}

void MyTumblerEngineBase::DoOnHardPassTimeout(const asio::error_code& code, bool cancel) {
    if (code) {
        CS_LOG_DEBUG("pass timer return code {}", code.value());
        return;
    }
    if (cancel) {
        CS_LOG_DEBUG("pass timer has been cancelled");
        return;
    }

    is_hard_pass_ = true;
    CheckCanPass();
}

void MyTumblerEngineBase::DoOnSoftPassTimeout(const asio::error_code& code, bool cancel) {
    if (code) {
        CS_LOG_DEBUG("pass timer return code {}", code.value());
        return;
    }
    if (cancel) {
        CS_LOG_DEBUG("pass timer has been cancelled");
        return;
    }

    if (passed_.find(my_id_) != passed_.end()) {
        return;
    }

    Pass();
}

// check if received enough pass
void MyTumblerEngineBase::CheckPassStatus() {
    if (passed_balance_ >= quorum_balance_) {
        forward_skip_timer_.Stop();
        if (passed_.count(my_id_)) {
            CETINA_GAUGE_SET(MYTUMBLER_PASS_TIME, time_utils::GetDuration(pass_start_time_));
        }

        // vote zero for instance in pending_ if necessary
        for (const auto& proposer : pending_[GetCurrSeq()]) {
            if (already_vote_zero_.find(proposer) != already_vote_zero_.end()) {
                continue;
            }
            auto myba = GetMyBA(proposer);
            if (myba == nullptr) {
                CS_LOG_ERROR("invalid proposer, should not happen");
                return;
            }
            myba->SwitchToNormalPath(GetCurrSeq(), proposer);
            already_vote_zero_.insert(proposer);
        }

        // for (const auto& proposer : ba_complete_) {
        //     auto myba = GetMyBA(proposer);
        //     myba->SwitchToNormalPath(GetCurrSeq(), proposer);
        // }

        // remove instance in pending_ if at least 2f+1 have not endorsed
        for (auto iter = pending_[GetCurrSeq()].begin(); iter != pending_[GetCurrSeq()].end();) {
            uint64_t not_endorsed_balance = 0;
            for (auto& pass : passed_) {
                if (std::find(pass.second->endorsed.Acquire().begin(),
                              pass.second->endorsed.Acquire().end(),
                              *iter)
                    == pass.second->endorsed.Acquire().end()) {
                    not_endorsed_balance += peers_balance_->at(pass.first);
                }
            }
            if (not_endorsed_balance >= quorum_balance_) {
                CS_LOG_INFO("removed proposal not endorsed by quorum, seq: {} proposer: {}",
                            GetCurrSeq(),
                            toHex(*iter).substr(0, 8));
                if (*iter == my_id_
                    && propose_state_.propose_seq_.load(std::memory_order_relaxed)
                           == GetCurrSeq()) {
                    // must first set last_failed_seq_ then is_proposed_, as CanPropose check in the
                    // reverse order
                    if (propose_state_.is_proposed_.load(std::memory_order_relaxed)) {
                        propose_state_.last_failed_seq_.store(GetCurrSeq(),
                                                              std::memory_order_relaxed);
                    }
                    // only advance if passed
                    if (passed_.find(my_id_) != passed_.end()) {
                        propose_state_.AdvanceProposeState(false, GetCurrSeq() + 1);
                    }
                }
                iter = pending_[GetCurrSeq()].erase(iter);
            } else {
                ++iter;
            }
        }

        CheckConsensusComplete();
    }
}

void MyTumblerEngineBase::DoOnConsensusComplete(const Seq seq, const Digest& random_seed) {
    CS_LOG_INFO("consensus completed, seq: {} contains {} proposals", seq, ba_success_.size());
    CETINA_GAUGE_SET(MYTUMBLER_SUCCESS_PROPOSAL_COUNT, ba_success_.size());
    CETINA_GAUGE_SET(MYTUMBLER_CONTRIBUTED_PROPOSER_COUNT, output_.size());
    bool my_proposal_success = ba_success_.count(my_id_);
    if (my_proposal_success) {
        CETINA_COUNTER_INC(MYTUMBLER_MY_SUCCESS_COUNT);
        ++proposal_success_count_;
    }
    std::vector<uint64_t> timestamps;
    std::vector<ssz_types::VoteStatus> all_vote_status;
    for (const auto& instance : ba_success_) {
        ssz_types::VoteStatus vote_status;
        auto val = val_[seq][instance.first][instance.second];
        timestamps.push_back(val->timestamp);
        if (instance.second == ZERO_32_BYTES) {
            continue;
        }
        vote_status.is_consensused = 1;
        auto proposal = std::make_shared<ssz_types::Proposal>();
        proposal->proposer = toBytes(NodeId(val->proposer_id.Acquire()));
        proposal->timestamp = val->timestamp;
        proposal->data = toBytes(std::string(val->payload.Acquire()));
        vote_status.proposal = proposal;
        for (auto& voter : voters_with_tx_[instance.first][instance.second]) {
            vote_status.voters.PushBack(toBytes(voter));
        }
        all_vote_status.emplace_back(std::move(vote_status));
    }
    if (val_[seq].find(my_id_) != val_[seq].end() && my_proposal_success) {
        for (const auto& instance : val_[seq][my_id_]) {
            ssz_types::VoteStatus vote_status;
            if (instance.first == ZERO_32_BYTES) {
                continue;
            }
            vote_status.is_consensused = 0;
            auto proposal = std::make_shared<ssz_types::Proposal>();
            proposal->proposer = toBytes(my_id_);
            proposal->timestamp = instance.second->timestamp;
            proposal->data = toBytes(std::string(instance.second->payload.Acquire()));
            vote_status.proposal = proposal;
            for (auto& voter : voters_with_tx_[my_id_][instance.first]) {
                vote_status.voters.PushBack(toBytes(voter));
            }
            all_vote_status.emplace_back(std::move(vote_status));
        }
    }

    // choose the median
    uint64_t ts = 0;
    if (timestamps.empty()) {
        ts = last_consensus_ts_.load() + hard_pass_timer_.GetTimeoutInterval();
        CS_LOG_WARN("empty proposal of seq {}, use timestamp {}", seq, ts);
    } else {
        std::sort(timestamps.begin(), timestamps.end());
        ts = timestamps[timestamps.size() / 2];
    }
    ts = std::max(last_consensus_ts_.load() + 1, ts);
    // should not over 5 minutes
    if (ts - last_consensus_ts_.load() > 300000) {
        ts = last_consensus_ts_.load() + 300000;
        CS_LOG_WARN("proposal timestamp of seq {} is too far, use timestamp {}", seq, ts);
    }
    // {is_epoch_end, to_persist}
    std::pair<bool, std::map<wal::EntryType, std::vector<bytes>>> consensus_result =
        consensus_finish_cb_(seq, ts, random_seed, all_vote_status);
    DoUpdateRawSeq(seq, ts, false, consensus_result.first);
    reliable_channel_->PersistRawBlock(seq, std::move(consensus_result.second));
    // send pass ack after persisting raw block
    for (const auto& pass_msg : passed_) {
        reliable_channel_->SendReceipt(
            pass_msg.first,
            std::make_shared<ssz_types::ConsensusMessage>(pass_msg.second));
    }
    ClearCurrSeqState(seq);
    if (!consensus_result.first) {  // epoch not end, continue
        if (propose_state_.propose_seq_.load(std::memory_order_relaxed) == seq) {
            if (propose_state_.is_proposed_.load(std::memory_order_relaxed)) {
                // maybe my check sanity is not finished, or removed quorum passed
                propose_state_.last_failed_seq_.store(seq, std::memory_order_relaxed);
            }
            propose_state_.AdvanceProposeState(false, seq + 1);
        }
        HandlePendingMessage(seq + 1);
    }

    UpdateConsensusFinishMetrics(seq);
}

// uint16_t MyTumblerEngineBase::Threshold_f() {
//     return f_ + 1;
// }

// uint16_t MyTumblerEngineBase::Threshold_quorum() {
//     return n_ - f_;
// }

void MyTumblerEngineBase::ReliableBroadcastMsg(const MessagePtr msg, bool need_save) {
    if (!msg) {
        return;
    }

    if (need_save) {
        reliable_channel_->SaveOutgoingMessage(msg);
    }
    reliable_channel_->BroadcastMessage(msg);
}

void MyTumblerEngineBase::SendMsg(const NodeId& node, const MessagePtr msg) {
    auto start_time = time_utils::GetSteadyTimePoint();

    send_(node, msg);
    if (msg->Index() == ConsensusMessageType::ValMessage) {
        CETINA_GAUGE_SET(MYTUMBLER_VAL_DURATION, time_utils::GetDuration(start_time));
    }
}

void MyTumblerEngineBase::BroadcastMsg(const MessagePtr msg) {
    auto start_time = time_utils::GetSteadyTimePoint();

    broadcast_(msg);
    if (msg->Index() == ConsensusMessageType::ValMessage) {
        CETINA_GAUGE_SET(MYTUMBLER_VAL_DURATION, time_utils::GetDuration(start_time));
    }
}

Seq MyTumblerEngineBase::GetCurrSeq() {
    return last_consensus_seq_.load() + 1;
}

NodeId MyTumblerEngineBase::GetLeader(const Seq seq) {
    size_t node_size = current_proposers_.size();
    if (node_size == 0) {
        CS_LOG_FATAL("proposer list is null!");
        return NodeId();
    }
    auto it = current_proposers_.begin();
    std::advance(it, (seq % node_size));
    return *it;
}

MyBAPtr MyTumblerEngineBase::GetMyBA(const NodeId& proposer) {
    if (peers_->find(proposer) == peers_->end()) {
        CS_LOG_ERROR("Node id does not exist, {}", toHex(proposer).substr(0, 8));
        return nullptr;
    }

    uint16_t index = reliable_channel_->GetNodeIndex(proposer) % myba_.size();
    return myba_[index];
}

void MyTumblerEngineBase::SetNode() {
    n_ = peers_->size();
    total_balance_ = 0;
    for (const auto& node_balance : *peers_balance_) {
        total_balance_ += node_balance.second;
    }
    if (total_balance_ > 0) {
        tolerable_faulty_balance_ = (total_balance_ - 1) / 3;

    } else {
        tolerable_faulty_balance_ = 0;
    }
    quorum_balance_ = total_balance_ - tolerable_faulty_balance_;

    CETINA_GAUGE_SET(MYTUMBLER_VALIDATOR_NUMBER, n_);
    CETINA_GAUGE_SET(MYTUMBLER_TOTAL_BALANCE, total_balance_ / 1000000);
    CS_LOG_INFO("node size: n = {},  max. faulty balance = {}, quorum balance = {}",
                n_,
                tolerable_faulty_balance_,
                quorum_balance_);

    reliable_channel_->SetNodes(peers_);
}

void MyTumblerEngineBase::HandlePendingMessage(const Seq seq) {
    CS_LOG_DEBUG("handle pending msg on seq {}", seq);
    if (!CanPipeline(seq)) {
        CS_LOG_DEBUG("msg out of range {} last_consensus {} last stable {}",
                     seq,
                     last_consensus_seq_.load(),
                     last_stable_seq_.load());
        return;
    }
    RecursiveGuard lock(pending_msg_lock_);
    auto it = pending_msg_.begin();
    while (it != pending_msg_.end()) {
        if (it->first.msg_info_.seq_ == seq) {
            OnConsensusMessage(it->first.sender_id_, it->second);
            it = pending_msg_.erase(it);
        } else if (it->first.msg_info_.seq_ < seq) {
            reliable_channel_->SendReceipt(it->first.sender_id_, it->second);
            it = pending_msg_.erase(it);
        } else {
            break;
        }
    }
}

bool MyTumblerEngineBase::SetConsensusThread(const uint16_t num) {
    myba_.clear();
    if (num == 0) {
        CS_LOG_ERROR("Number of threads is zero");
        return false;
    }

    for (auto i = 0; i < num; ++i) {
        MyBAPtr myba = std::make_shared<MyBA>(
            spec_version_,
            epoch_number_,
            my_id_,
            n_,
            tolerable_faulty_balance_,
            quorum_balance_,
            last_stable_seq_.load(),
            crypto_helper_,
            peers_,
            peers_balance_,
            [this](const Seq seq, const NodeId& proposer, const Digest& hash) {
                EndorseCallBack(seq, proposer, hash);
            },
            [this](const Seq seq, const NodeId& proposer, const Digest& hash, const NodeId& voter) {
                RecordVoterCallBack(seq, proposer, hash, voter);
            },
            [this](const Seq seq, const NodeId& proposer, const Digest& hash) {
                MyBACompleteCallBack(seq, proposer, hash);
            },
            reliable_channel_);
        myba_.push_back(myba);
    }

    return true;
}

void MyTumblerEngineBase::GarbageCollection(const Seq seq) {
    val_.erase(seq);
    unchecked_proposals_.erase(seq);

    for (auto& ba : myba_) {
        ba->GarbageCollection(seq);
    }

    reliable_channel_->GarbageCollection(seq);

    Seq init_seq = 0;
    if (seq > cfg_.consensus_window_) {
        init_seq = seq - cfg_.consensus_window_;
    }
    stable_proofs_.erase(stable_proofs_.begin(), stable_proofs_.upper_bound(init_seq));
    Seq finished_proof_gc = std::min(last_sent_proof_seq_, seq);
    my_finished_proofs_.erase(my_finished_proofs_.begin(),
                              my_finished_proofs_.upper_bound(finished_proof_gc));
}

void MyTumblerEngineBase::DoOnRequestProposalTimeout(const asio::error_code& code, bool cancel) {
    if (code) {
        CS_LOG_DEBUG("request proposal timer return code {}", code.value());
        return;
    }
    if (cancel) {
        CS_LOG_DEBUG("request proposal timer has been cancelled");
        return;
    }

    if (last_stable_update_time_ + 5000 < time_utils::GetCurrentTimestamp()) {
        BroadcastProofs();
        PrintDiagnostics();
    }

    auto seq = GetCurrSeq();
    std::vector<ssz_types::ProposalRequestKey> keys;
    for (const auto& instance : ba_success_) {
        if (val_[seq].find(instance.first) == val_[seq].end()
            || val_[seq][instance.first].find(instance.second) == val_[seq][instance.first].end()) {
            ssz_types::ProposalRequestKey key;
            key.proposer_id = instance.first;
            key.hash = instance.second;
            keys.emplace_back(std::move(key));
            CS_LOG_INFO("request proposal from {} hash: {}",
                        toHex(instance.first).substr(0, 8),
                        toHex(instance.second).substr(0, 8));
        }
    }
    if (keys.empty()) {
        request_proposal_timer_.Reset();
        return;
    }
    auto request = CreateRequestProposalMessage(seq, std::move(keys));
    BroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(request));
    CS_LOG_DEBUG("request proposal at {}", seq);
    request_proposal_timer_.Reset();
}

void MyTumblerEngineBase::DoOnResendProofTimeout(const asio::error_code& code, bool cancel) {
    if (code) {
        CS_LOG_DEBUG("resend proof timer return code {}", code.value());
        return;
    }
    if (cancel) {
        CS_LOG_DEBUG("resend proof timer has been cancelled");
        return;
    }

    BroadcastProofs();
    auto cur_timeout = std::min(resend_proof_timer_.GetTimeoutInterval() + 50, 500u);
    resend_proof_timer_.ResetTimeoutInterval(cur_timeout);
    resend_proof_timer_.Reset();
}

void MyTumblerEngineBase::BroadcastProofs() {
    SkipMessagePtr skip_msg = CreateSkipMessage(0, my_id_, Signature());
    Seq init_stable = 0;
    if (last_stable_seq_.load() > cfg_.consensus_window_) {
        init_stable = last_stable_seq_.load() - cfg_.consensus_window_;
    }
    for (auto itr = stable_proofs_.lower_bound(init_stable); itr != stable_proofs_.end(); ++itr) {
        skip_msg->encoded_stable_proof.PushBack(itr->second);
    }
    if (!my_finished_proofs_.empty()) {
        Seq lb = std::min(last_stable_seq_.load(), last_sent_proof_seq_);
        for (auto itr = my_finished_proofs_.upper_bound(lb); itr != my_finished_proofs_.end();
             ++itr) {
            for (const auto& my_proof : itr->second) {
                skip_msg->encoded_block_checkpoint_info.PushBack(my_proof);
            }
        }
        last_sent_proof_seq_ = my_finished_proofs_.rbegin()->first;
    }

    // no need to sign. will be ignored after handling proofs
    CS_LOG_INFO(
        "send checkpoint after epoch end or long time no stable: last stable = {} last finished = "
        "{}",
        last_stable_seq_.load(),
        last_finished_seq_);
    BroadcastMsg(std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
}

int MyTumblerEngineBase::DoOnRecvRequestProposalMessage(const NodeId& sender,
                                                        const RequestProposalMessagePtr msg) {
    if (msg->seq <= last_stable_seq_.load()) {
        CS_LOG_DEBUG("old request seq: {} last stable: {}", msg->seq, last_stable_seq_.load());
        return -1;
    }
    if (msg->keys.Size() == 0) {
        return -2;
    }
    CS_LOG_DEBUG("recv request proposal from {}", toHex(sender).substr(0, 8));
    std::vector<ValMessagePtr> vals;
    for (const auto& key : msg->keys.Acquire()) {
        NodeId nodeid_str = NodeId(key.proposer_id.Acquire());
        Digest hash_str = Digest(key.hash.Acquire());
        if (val_.find(msg->seq) != val_.end()
            && val_[msg->seq].find(nodeid_str) != val_[msg->seq].end()
            && val_[msg->seq][nodeid_str].find(hash_str) != val_[msg->seq][nodeid_str].end()) {
            vals.emplace_back(val_[msg->seq][nodeid_str][hash_str]);
            CS_LOG_DEBUG("response seq: {} , proposer: {} , hash: {}",
                         msg->seq,
                         key.proposer_id.Hex().substr(0, 8),
                         key.hash.Hex().substr(0, 8));
        }
    }
    if (vals.empty()) {
        return -3;
    }
    auto response = std::make_shared<ssz_types::ResponseProposalMessage>();
    response->vals = std::move(vals);
    SendMsg(sender, std::make_shared<ssz_types::ConsensusMessage>(response));
    return 0;
}

int MyTumblerEngineBase::DoOnRecvResponseProposalMessage(const NodeId& sender,
                                                         const ResponseProposalMessagePtr msg) {
    if (msg->vals.Size() == 0) {
        return -1;
    }
    CS_LOG_DEBUG("recv response from {}", toHex(sender).substr(0, 8));
    auto seq = GetCurrSeq();
    for (const auto& val : msg->vals.Acquire()) {
        if (val == nullptr || val->seq != seq) {
            return -2;
        }
        NodeId nodeid_str = NodeId(val->proposer_id.Acquire());
        Digest hash_str = Digest(val->hash.Acquire());
        if (ba_success_.count(nodeid_str)
            && val_[seq][nodeid_str].find(hash_str) == val_[seq][nodeid_str].end()) {
            if (ba_success_[nodeid_str] != hash_str
                || CalculateValMessageHash(val) != Digest(val->hash.Acquire())) {
                CS_LOG_ERROR("invalid val hash");
                return -3;
            }
            if (!CheckMessageSanity(seq, nodeid_str, CheckSanityType::PULLED_FROM_OTHERS, val)) {
                CS_LOG_ERROR("check validity failed");
                return -4;
            }
        }
    }
    return 0;
}

void MyTumblerEngineBase::PrintDiagnostics() {
    // last_stable, last_finish, last_consensus_seq_, is_epoch_end, received passes, pendings: my_ba
    // decided? has val?
    std::stringstream ss;
    ss << "last stable seq: " << last_stable_seq_.load()
       << ", last finished seq: " << last_finished_seq_
       << ", last consensus seq: " << last_consensus_seq_.load()
       << ", is epoch end: " << is_epoch_end_.load();
    if (passed_balance_ < quorum_balance_) {
        ss << ", passed balance " << passed_balance_ << " not reach quorum balance "
           << quorum_balance_ << ", passed nodes: [";
        for (const auto& nodeid : passed_) {
            ss << toHex(nodeid.first).substr(0, 8) << ", ";
        }
        ss << "]";
    } else {
        ss << ", pass messages enough";
    }
    if (!pending_[GetCurrSeq()].empty()) {
        ss << ", has pending proposal: [";
        for (const auto& node : pending_[GetCurrSeq()]) {
            ss << "{" << toHex(node).substr(0, 8);
            if (ba_complete_.count(node)) {
                ss << ", myba complete with " << ba_success_.count(node);
            } else {
                ss << ", myba not complete";
            }
            ss << ", has val: " << (val_[GetCurrSeq()].count(node) > 0) << "}, ";
        }
        ss << "]";
    } else {
        ss << ", no pending proposal";
    }

    CS_LOG_WARN("long time not stable, {}", ss.str());
}

void MyTumblerEngineBase::UpdateConsensusStartTime(const Seq seq) {
    auto start_time = time_utils::GetSteadyTimePoint();
    consensus_start_time_[seq] = start_time;
    auto duration = time_utils::GetDuration(last_rawblock_finish_time_, start_time);
    CETINA_GAUGE_SET(MYTUMBLER_IDLE_TIME, duration);
}

void MyTumblerEngineBase::UpdateConsensusFinishMetrics(const Seq seq) {
    auto cur_finish_time = time_utils::GetSteadyTimePoint();
    auto itr = consensus_start_time_.find(seq);
    if (itr != consensus_start_time_.end()) {
        auto duration = time_utils::GetDuration(itr->second, cur_finish_time);
        CETINA_GAUGE_SET(MYTUMBLER_CONSENSUS_TIME, duration);
    } else {
        auto duration = time_utils::GetDuration(consensus_finish_time_, cur_finish_time);
        CETINA_GAUGE_SET(MYTUMBLER_CONSENSUS_TIME, duration);
    }
    consensus_start_time_.erase(consensus_start_time_.begin(), itr);
    consensus_finish_time_ = cur_finish_time;
}

std::pair<ConsensusStatsError, std::optional<ConsensusStats>>
MyTumblerEngineBase::GetConsensusStats() const {
    if (!engine_stats_configured_) {
        return {ConsensusStatsError::kNotInitialized, std::nullopt};
    }
    if (stopped_) {
        return {ConsensusStatsError::kEngineStopped, std::nullopt};
    }
    // RPC thread only: post queues work after the current consensus handler; get() blocks this
    // (non-executor) thread until the snapshot runs. Do not call from consensus_io_ctx_ handlers.
    std::promise<std::pair<ConsensusStatsError, std::optional<ConsensusStats>>> prom;
    auto fut = prom.get_future();
    consensus_io_ctx_->post([this, &prom]() {
        prom.set_value(BuildConsensusStatsSnapshot());
    });
    return fut.get();
}

std::pair<ConsensusStatsError, std::optional<ConsensusStats>>
MyTumblerEngineBase::BuildConsensusStatsSnapshot() const {
    if (!engine_stats_configured_) {
        return {ConsensusStatsError::kNotInitialized, std::nullopt};
    }
    if (stopped_) {
        return {ConsensusStatsError::kEngineStopped, std::nullopt};
    }
    ConsensusStats out;
    out.stats_schema_revision = 1;
    out.consensus_version = spec_version_;
    const Seq baseline = stats_baseline_last_consensus_seq_;
    const Seq last = last_consensus_seq_.load();
    out.engine_stats_block_count = last >= baseline ? last - baseline : 0;
    out.latest_block_number = last;
    out.validator_number = n_;
    out.self_consensus_completed_by_sync_count = sync_completed_count_;
    out.self_proposal_success_count = proposal_success_count_;
    out.self_proposal_rejected_count = proposal_rejected_count_;
    out.self_skip_count = skip_stats_count_;
    Guard lock(inactive_proposers_lock_);
    for (const auto& e : peer_inactive_events_) {
        out.peer_stats[e.first].inactive_count = e.second;
    }
    return {ConsensusStatsError::kOk, std::move(out)};
}

}  // namespace consensus_spec
