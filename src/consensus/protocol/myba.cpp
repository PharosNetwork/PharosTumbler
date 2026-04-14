// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/protocol/myba.h"

#include <pamir/cetina/cetina.h>
#include "consensus/libraries/common/const_define.h"
#include "consensus/libraries/log/consensus_log.h"

namespace consensus_spec {

MyBA::MyBA(const uint16_t spec_version,
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
           ReliableChannelBasePtr reliable_channel)
        : Worker("myba", 1),
          spec_version_(spec_version),
          epoch_number_(epoch_number),
          my_id_(my_id),
          n_(n),
          tolerable_faulty_balance_(tolerable_faulty_balance),
          quorum_balance_(quorum_balance),
          stable_seq_(stable_seq),
          crypto_helper_(crypto_helper),
          peers_(peers),
          peers_balance_(peers_balance),
          endorse_cb_(endorse_cb),
          record_voter_cb_(record_voter_cb),
          ba_complete_cb_(ba_complete_cb),
          reliable_channel_(reliable_channel) {}

void MyBA::LoadLog(const Seq seq,
                   const NodeId& proposer,
                   const std::vector<std::pair<NodeId, MessagePtr>>& msgs) {
    auto msg_pool = GetMsgPool(ProposalKey(seq, proposer));
    std::set<Digest> values;
    for (auto& msg : msgs) {
        switch (msg.second->Index()) {
            case ConsensusMessageType::AggregatedBvalMessage: {
                auto aggregated_bval_msg = msg.second->AggregatedBvalData();
                if (aggregated_bval_msg == nullptr) {
                    CS_LOG_ERROR("invalid aggregated bval msg in log");
                    break;
                }

                CS_LOG_INFO("have aggregated_bval from {} seq: {} proposer: {}, round {}, hash: {}",
                            toHex(msg.first).substr(0, 8),
                            aggregated_bval_msg->seq,
                            aggregated_bval_msg->proposer_id.Hex().substr(0, 8),
                            aggregated_bval_msg->round,
                            aggregated_bval_msg->hash.Hex().substr(0, 8));
                PushTask([this, msg]() {
                    OnRecvAggregatedBvalMessage(msg.first, msg.second);
                });
                break;
            }
            case ConsensusMessageType::PromMessage: {
                auto prom_msg = msg.second->PromData();
                if (prom_msg == nullptr) {
                    CS_LOG_ERROR("invalid prom msg in log");
                    return;
                }
                CS_LOG_INFO("have prom from {}, proposer: {}, round: {}, hash: {}",
                            toHex(msg.first).substr(0, 8),
                            prom_msg->proposer_id.Hex().substr(0, 8),
                            prom_msg->round,
                            prom_msg->hash.Hex().substr(0, 8));
                msg_pool->AddPromMsg(msg.first, prom_msg);
                values.insert(Digest(prom_msg->hash.Acquire()));
                break;
            }
            case ConsensusMessageType::AuxMessage: {
                auto aux_msg = msg.second->AuxData();
                if (aux_msg == nullptr) {
                    CS_LOG_ERROR("invalid aux msg in log");
                    return;
                }
                CS_LOG_INFO("have aux from {} proposer: {}, round: {}, hash: {}",
                            toHex(msg.first).substr(0, 8),
                            aux_msg->proposer_id.Hex().substr(0, 8),
                            aux_msg->round,
                            aux_msg->hash.Hex().substr(0, 8));
                msg_pool->AddAuxMsg(msg.first, aux_msg);
                values.insert(Digest(aux_msg->hash.Acquire()));
                break;
            }
            case ConsensusMessageType::AggregatedMainVoteMessage: {
                auto aggregated_mainvote_msg = msg.second->AggregatedMainVoteData();
                if (aggregated_mainvote_msg == nullptr) {
                    CS_LOG_ERROR("invalid aggregated main vote msg in log");
                    break;
                }

                CS_LOG_INFO("have aggregated_mainvote from {} seq: {} proposer: {}, round: {}",
                            toHex(msg.first).substr(0, 8),
                            aggregated_mainvote_msg->seq,
                            aggregated_mainvote_msg->proposer_id.Hex().substr(0, 8),
                            aggregated_mainvote_msg->round);
                CS_LOG_INFO("aux info size {}", aggregated_mainvote_msg->aggr_aux_infos.Size());
                CS_LOG_INFO("prom info size {}", aggregated_mainvote_msg->aggr_prom_infos.Size());
                reliable_channel_->BroadcastMessage(msg.second);
                OnRecvAggregatedMainVoteMessage(msg.first, msg.second, true);
                break;
            }
            default: {
                CS_LOG_ERROR("Invalid msg type {}", msg.second->Index());
                break;
            }
        }
    }

    for (auto& hash : values) {
        CheckBvalStatus(msg_pool, hash);
        if (LegacyVersionBeforeDecisionProof()) {
            if (msg_pool->IsLegacyCompleted()) {
                return;
            }
        }

        if (msg_pool->IsDecided()) {
            return;
        }
    }
    return;
}

int MyBA::OnRecvMessage(const NodeId& sender, const MessagePtr msg) {
    if (!msg) {
        return -1;
    }

    int ret = -99;

    switch (msg->Index()) {
        case ConsensusMessageType::BvalMessage: {
            ret = OnRecvBvalMessage(sender, msg);
            break;
        }
        case ConsensusMessageType::AggregatedBvalMessage: {
            ret = OnRecvAggregatedBvalMessage(sender, msg);
            break;
        }
        case ConsensusMessageType::PromMessage: {
            ret = OnRecvPromMessage(sender, msg);
            break;
        }
        case ConsensusMessageType::AuxMessage: {
            ret = OnRecvAuxMessage(sender, msg);
            break;
        }
        case ConsensusMessageType::AggregatedMainVoteMessage: {
            ret = OnRecvAggregatedMainVoteMessage(sender, msg);
            break;
        }
        default: {
            CS_LOG_ERROR("Invalid msg type {}", msg->Index());
            break;
        }
    }

    return ret;
}

void MyBA::GarbageCollection(const Seq seq) {
    if (stable_seq_.load() < seq) {
        stable_seq_.store(seq);
    }
    PushTask([this, seq]() {
        this->DoGarbageCollection(seq);
    });
}

void MyBA::Start() {
    StartWorking();
}

void MyBA::Stop() {
    StopWorking();
}

void MyBA::PushMessage(const NodeId& sender, const MessagePtr msg) {
    msg_queue_.Push({sender, msg});
}

void MyBA::Endorse(const Seq seq, const NodeId& proposer, const Digest& hash, bool val_exist) {
    PushTask([this, seq, proposer, hash, val_exist]() {
        this->DoEndorse(seq, proposer, hash, val_exist);
    });
}

void MyBA::SwitchToNormalPath(const Seq seq, const NodeId& proposer) {
    PushTask([this, seq, proposer]() {
        this->DoSwitchToNormalPath(seq, proposer);
    });
}

void MyBA::DoWork() {
    MessageQueueItem item;
    int count = 0;
    while (count < 100 && msg_queue_.Pop(item)) {
        OnRecvMessage(item.sender_, item.msg_);
        ++count;
    }
    if (count > 0) {
        CS_LOG_INFO("Message queue processed {}, left {}", count, msg_queue_.Size());
    }
}

void MyBA::DoEndorse(const Seq seq, const NodeId& proposer, const Digest& hash, bool val_exist) {
    if (seq <= stable_seq_.load()) {
        return;
    }

    auto msg_pool = GetMsgPool(ProposalKey(seq, proposer));
    CS_LOG_INFO("endorse seq {}, proposer {}, digest {}, val_exist {}",
                seq,
                toHex(proposer).substr(0, 8),
                toHex(hash).substr(0, 8),
                val_exist);

    if (msg_pool->ExistBval(hash, 0, my_id_)) {
        return;
    }

    if (msg_pool->HaveBvalOther(0, hash)) {
        return;
    }

    if (hash == ZERO_32_BYTES) {
        msg_pool->SwitchToNormalPath();
    }

    if (msg_pool->GetCurrentRound() == 0 && !msg_pool->HavePromAny(0).first
        && !msg_pool->HaveAuxAny(0).first
        && msg_pool->GetBvalBalance(hash, 0, *peers_balance_) < quorum_balance_) {
        auto vote = CreateBvalMessage(seq, proposer, 0, hash, val_exist, epoch_number_);
        std::string tmp_sig;
        crypto_helper_->AggregateSign(CalculateBvalDigest(vote, spec_version_), tmp_sig);
        vote->signature = std::move(tmp_sig);
        auto vote_msg = std::make_shared<ssz_types::ConsensusMessage>(vote);
        if (!msg_pool->IsNormalPath()) {
            SendMessage(proposer, vote_msg);
        } else {
            BroadcastMessage(vote_msg);
        }
        OnRecvBvalMessage(my_id_, vote_msg);
    }
}

void MyBA::DoSwitchToNormalPath(const Seq seq, const NodeId& proposer) {
    if (seq <= stable_seq_.load()) {
        return;
    }

    auto msg_pool = GetMsgPool(ProposalKey(seq, proposer));
    if (msg_pool->GetCurrentRound() != 0) {
        return;
    }

    if (msg_pool->IsNormalPath()) {
        return;
    }

    msg_pool->SwitchToNormalPath();

    AggregatedMainVoteMessagePtr main_vote;
    if (!LegacyVersionBeforeDecisionProof()
        && msg_pool->GetDecisionAggregatedMainVoteMsg(main_vote)) {
        CS_LOG_INFO("switch to normal path, re-broadcast DecisionCertificate seq {}, proposer {}",
                    seq,
                    toHex(proposer).substr(0, 8));
        BroadcastMessage(std::make_shared<ssz_types::ConsensusMessage>(main_vote));
        return;
    } else if (msg_pool->GetLatestAggregatedMainVoteMsg(main_vote)) {
        CS_LOG_INFO("switch to normal path, re-broadcast AggregatedMainVote seq {}, proposer {}",
                    seq,
                    toHex(proposer).substr(0, 8));
        BroadcastMessage(std::make_shared<ssz_types::ConsensusMessage>(main_vote));
        return;
    }

    const auto& prom_result = msg_pool->HavePromAny(0);
    if (prom_result.first && prom_result.second != nullptr) {
        CS_LOG_INFO("switch to normal path, re-broadcast Prom seq {}, proposer {}",
                    seq,
                    toHex(proposer).substr(0, 8));
        BroadcastMessage(std::make_shared<ssz_types::ConsensusMessage>(prom_result.second));
        return;
    } else {
        const auto& aux_result = msg_pool->HaveAuxAny(0);
        if (aux_result.first && aux_result.second != nullptr) {
            CS_LOG_INFO("switch to normal path, re-broadcast Aux seq {}, proposer {}",
                        seq,
                        toHex(proposer).substr(0, 8));
            BroadcastMessage(std::make_shared<ssz_types::ConsensusMessage>(aux_result.second));
            return;
        }
    }

    CS_LOG_INFO("switch to normal path, broadcast both zero and non-zero seq {}, proposer {}",
                seq,
                toHex(proposer).substr(0, 8));

    static Digest zero = ZERO_32_BYTES;

    if (msg_pool->ExistBval(zero, 0, my_id_)) {
        return;
    }

    auto other_bval = msg_pool->GetBvalOtherMessage(0, zero);
    if (other_bval.first && other_bval.second != nullptr) {
        BroadcastMessage(std::make_shared<ssz_types::ConsensusMessage>(other_bval.second));
    }

    auto bval = CreateBvalMessage(seq, proposer, 0, zero, false, epoch_number_);
    std::string tmp_sig;
    crypto_helper_->AggregateSign(CalculateBvalDigest(bval, spec_version_), tmp_sig);
    bval->signature = std::move(tmp_sig);
    auto bval_msg = std::make_shared<ssz_types::ConsensusMessage>(bval);
    BroadcastMessage(bval_msg);
    OnRecvBvalMessage(my_id_, bval_msg);

    return;
}

void MyBA::DoGarbageCollection(const Seq seq) {
    for (auto it = ba_msg_pool_.begin(); it != ba_msg_pool_.end();) {
        if (it->first.seq_ > seq) {
            break;
        }
        it = ba_msg_pool_.erase(it);
    }
}

void MyBA::SendMessage(const NodeId& node_id, const MessagePtr msg) {
    if (!msg) {
        return;
    }
    if (n_ <= 1) {
        return;
    }

    if (msg->Index() != ConsensusMessageType::BvalMessage) {
        reliable_channel_->SaveOutgoingMessage(msg);
    }

    reliable_channel_->SendMessage(node_id, msg);
}

void MyBA::BroadcastMessage(const MessagePtr msg) {
    if (!msg) {
        return;
    }
    if (n_ <= 1) {
        return;
    }

    if (msg->Index() != ConsensusMessageType::BvalMessage) {
        reliable_channel_->SaveOutgoingMessage(msg);
    }
    reliable_channel_->BroadcastMessage(msg);
}

int MyBA::OnRecvBvalMessage(const NodeId& sender, const MessagePtr msg) {
    auto bval_msg = msg->BvalData();
    if (bval_msg == nullptr) {
        return -1;
    }

    if (bval_msg->seq <= stable_seq_.load()) {
        return -10;
    }

    CS_LOG_INFO("receive bval from {} seq: {} proposer: {} hash: {}, val exist: {}, round {}",
                toHex(sender).substr(0, 8),
                bval_msg->seq,
                bval_msg->proposer_id.Hex().substr(0, 8),
                bval_msg->hash.Hex().substr(0, 8),
                bval_msg->val_exist,
                bval_msg->round);

    NodeId nodeid_str = NodeId(bval_msg->proposer_id.Acquire());
    Digest hash_str = Digest(bval_msg->hash.Acquire());
    // bool from_proposer =
    //     sender == (bval_msg->proposer_id) && bval_msg->round == 0;
    auto msg_pool = GetMsgPool(ProposalKey(bval_msg->seq, nodeid_str));

    if (CheckCompleted(sender, msg_pool)) {
        reliable_channel_->SendReceipt(sender, msg);
        return -6;
    }

    // outdated msg
    if (bval_msg->round < msg_pool->GetCurrentRound()) {
        reliable_channel_->SendReceipt(sender, msg);
        return -3;
    }

    // redundant msg
    // only send receipt when ba up to next phase since bval msg not persist
    if (LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsLegacyCompleted() || msg_pool->HavePromAny(bval_msg->round).first
            || msg_pool->HaveAuxAny(bval_msg->round).first) {
            reliable_channel_->SendReceipt(sender, msg);
            return -4;
        }
    }
    if (msg_pool->HavePromAny(bval_msg->round).first
        || msg_pool->HaveAuxAny(bval_msg->round).first) {
        reliable_channel_->SendReceipt(sender, msg);
        return -4;
    }
    if (msg_pool->ExistBval(hash_str, bval_msg->round, sender)) {
        return -4;
    }

    // if (!from_proposer) {
    //     reliable_channel_->OnRecvMessage(sender, msg);
    // }
    msg_pool->AddBvalMsg(sender, bval_msg);
    if (bval_msg->val_exist == 1 && sender != bval_msg->proposer_id.Acquire()) {
        record_voter_cb_(bval_msg->seq, nodeid_str, hash_str, sender);
    }

    if (msg_pool->GetCurrentRound() == bval_msg->round) {
        CheckBvalStatus(msg_pool, hash_str);
    }

    return 0;
}

int MyBA::OnRecvAggregatedBvalMessage(const NodeId& sender, const MessagePtr msg) {
    auto aggregated_bval_msg = msg->AggregatedBvalData();
    if (aggregated_bval_msg == nullptr) {
        return -1;
    }

    if (aggregated_bval_msg->seq <= stable_seq_.load()) {
        return -10;
    }

    CS_LOG_INFO("receive aggregated_bval from {} seq: {} proposer: {} hash: {}, round {}",
                toHex(sender).substr(0, 8),
                aggregated_bval_msg->seq,
                aggregated_bval_msg->proposer_id.Hex().substr(0, 8),
                aggregated_bval_msg->hash.Hex().substr(0, 8),
                aggregated_bval_msg->round);

    Seq seq = aggregated_bval_msg->seq;
    Round round = aggregated_bval_msg->round;
    NodeId nodeid_str = NodeId(aggregated_bval_msg->proposer_id.Acquire());
    Digest hash_str = Digest(aggregated_bval_msg->hash.Acquire());

    auto msg_pool = GetMsgPool(ProposalKey(aggregated_bval_msg->seq, nodeid_str));

    if (CheckCompleted(sender, msg_pool)) {
        reliable_channel_->SendReceipt(sender, msg);
        return -6;
    }

    if (LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsLegacyCompleted()
            || msg_pool->ExistAggregatedBval(aggregated_bval_msg->round)) {
            // reliable_channel_->SendReceipt(sender, msg);
            return -3;
        }
    }

    if (msg_pool->ExistAggregatedBval(aggregated_bval_msg->round)) {
        // reliable_channel_->SendReceipt(sender, msg);
        return -3;
    }

    if (sender != my_id_
        && (!CheckQuorumBalance(aggregated_bval_msg->proof)
            || !VerifyPhaseOneQC(seq, nodeid_str, round, hash_str, aggregated_bval_msg->proof))) {
        CS_LOG_ERROR("Invalid proof in aggregated_bval msg");
        return -4;
    }

    msg_pool->AddAggregatedBvalMsg(sender, aggregated_bval_msg);
    msg_pool->AddValidValue(round, hash_str);
    DoPhaseTwoVote(msg_pool, sender, seq, nodeid_str, round, hash_str, aggregated_bval_msg->proof);

    reliable_channel_->SendReceipt(sender, msg);

    return 0;
}

int MyBA::OnRecvAggregatedMainVoteMessage(const NodeId& sender,
                                          const MessagePtr msg,
                                          bool is_restored) {
    auto aggregated_mainvote_msg = msg->AggregatedMainVoteData();
    if (aggregated_mainvote_msg == nullptr) {
        return -1;
    }

    if (aggregated_mainvote_msg->seq <= stable_seq_.load()) {
        return -10;
    }

    CS_LOG_INFO("receive aggregated_mainvote from {} seq: {} proposer: {}, round {}",
                toHex(sender).substr(0, 8),
                aggregated_mainvote_msg->seq,
                aggregated_mainvote_msg->proposer_id.Hex().substr(0, 8),
                aggregated_mainvote_msg->round);

    NodeId proposer = NodeId(aggregated_mainvote_msg->proposer_id.Acquire());

    auto msg_pool = GetMsgPool(ProposalKey(aggregated_mainvote_msg->seq, proposer));

    if (LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsLegacyCompleted()) {
            reliable_channel_->SendReceipt(sender, msg);
            return -3;
        }

        if (msg_pool->ExistAggregatedMainVote(aggregated_mainvote_msg->round)
            && aggregated_mainvote_msg->aggr_aux_infos.Size() > 0) {
            // if have already receive other aggregated main, only pure proms can help me
            // TODO: if only have a single valid value can also help me to decide
            reliable_channel_->SendReceipt(sender, msg);
            return -3;
        }
        if (sender != my_id_ && !VerifyAggregatedMainVote(aggregated_mainvote_msg)) {
            CS_LOG_ERROR("Invalid proof in aggregated_mainvote msg");
            return -4;
        }
    } else {
        // check this can help me decide
        if (msg_pool->IsDecided()) {
            reliable_channel_->SendReceipt(sender, msg);
            return -3;
        }
        // Always verify proofs from remote peers before any decide path, including old-round
        // shortcut handling.
        if (sender != my_id_ && !VerifyAggregatedMainVote(aggregated_mainvote_msg)) {
            CS_LOG_ERROR("Invalid proof in aggregated_mainvote msg");
            return -4;
        }
        std::set<Digest> received_values;
        if (aggregated_mainvote_msg->round < msg_pool->GetCurrentRound()
            || msg_pool->ExistAggregatedMainVote(aggregated_mainvote_msg->round)) {
            // from old round, can also help me to decide
            for (const auto& aux_info : aggregated_mainvote_msg->aggr_aux_infos.Acquire()) {
                received_values.emplace(aux_info.hash.Acquire());
            }
            for (const auto& prom_info : aggregated_mainvote_msg->aggr_prom_infos.Acquire()) {
                received_values.emplace(prom_info.hash.Acquire());
            }

            if (received_values.size() == 1) {
                if (aggregated_mainvote_msg->aggr_aux_infos.Size() == 0) {
                    // pure prom, fast decide without coin
                    Decide(true,
                           aggregated_mainvote_msg->round,
                           msg_pool,
                           Digest(aggregated_mainvote_msg->aggr_prom_infos[0].hash.Acquire()),
                           aggregated_mainvote_msg);
                } else {
                    const Digest& next_val = *received_values.begin();
                    bool coin = GetCoin(aggregated_mainvote_msg->round);
                    if (coin && next_val != ZERO_32_BYTES) {
                        Decide(false,
                               msg_pool->GetCurrentRound(),
                               msg_pool,
                               next_val,
                               aggregated_mainvote_msg);
                    } else if (!coin && next_val == ZERO_32_BYTES) {
                        Decide(false,
                               msg_pool->GetCurrentRound(),
                               msg_pool,
                               ZERO_32_BYTES,
                               aggregated_mainvote_msg);
                    }
                }
            }
            if (msg_pool->IsDecided()) {
                BroadcastMessage(
                    std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote_msg));
                reliable_channel_->SendReceipt(sender, msg);
                return 0;
            } else {
                reliable_channel_->SendReceipt(sender, msg);
                return -3;
            }
        }  // finish handling old round main vote
    }

    if (!is_restored && sender != my_id_) {  // actually stored must be my_id_
        // need persist
        reliable_channel_->OnRecvMessage(sender, msg);
    }

    msg_pool->AddAggregatedMainVoteMsg(sender, aggregated_mainvote_msg);

    std::set<Digest> valid_values;
    msg_pool->GetValidValues(msg_pool->GetCurrentRound(), valid_values);
    for (auto& valid_value : valid_values) {
        auto& auxs = msg_pool->GetAuxMessages(msg_pool->GetCurrentRound(), valid_value);
        for (const auto& aux : auxs) {
            reliable_channel_->SendReceipt(
                aux.first,
                std::make_shared<ssz_types::ConsensusMessage>(aux.second));
        }
        auto& proms = msg_pool->GetPromMessages(msg_pool->GetCurrentRound(), valid_value);
        for (const auto& prom : proms) {
            reliable_channel_->SendReceipt(
                prom.first,
                std::make_shared<ssz_types::ConsensusMessage>(prom.second));
        }
    }

    if (aggregated_mainvote_msg->aggr_aux_infos.Size() == 0
        && aggregated_mainvote_msg->aggr_prom_infos.Size() == 1) {
        Decide(true,
               aggregated_mainvote_msg->round,
               msg_pool,
               Digest(aggregated_mainvote_msg->aggr_prom_infos[0].hash.Acquire()),
               aggregated_mainvote_msg);
        if (proposer != sender && !is_restored) {
            // I decide not by the correct proposer, help to disseminate
            BroadcastMessage(
                std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote_msg));
        }
        return 0;
    }

    if (msg_pool->GetCurrentRound() < aggregated_mainvote_msg->round) {
        valid_values.clear();
    }
    for (const auto& aux_info : aggregated_mainvote_msg->aggr_aux_infos.Acquire()) {
        Digest received_value(aux_info.hash.Acquire());
        if (valid_values.count(received_value) == 0) {
            valid_values.emplace(received_value);
        }
    }
    for (const auto& prom_info : aggregated_mainvote_msg->aggr_prom_infos.Acquire()) {
        Digest received_value(prom_info.hash.Acquire());
        if (valid_values.count(received_value) == 0) {
            valid_values.emplace(received_value);
        }
    }

    bool coin = GetCoin(aggregated_mainvote_msg->round);
    Digest next_val;
    if (valid_values.size() == 1) {
        next_val = *(valid_values.begin());
        if (coin && next_val != ZERO_32_BYTES) {
            Decide(false, msg_pool->GetCurrentRound(), msg_pool, next_val, aggregated_mainvote_msg);
            if (!LegacyVersionBeforeDecisionProof()) {
                BroadcastMessage(
                    std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote_msg));
            }
        } else if (!coin && next_val == ZERO_32_BYTES) {
            Decide(false,
                   msg_pool->GetCurrentRound(),
                   msg_pool,
                   ZERO_32_BYTES,
                   aggregated_mainvote_msg);
            if (!LegacyVersionBeforeDecisionProof()) {
                BroadcastMessage(
                    std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote_msg));
            }
        }
    } else {
        if (!coin) {
            next_val = ZERO_32_BYTES;
        } else {
            for (auto& it : valid_values) {
                if (it != ZERO_32_BYTES) {
                    next_val = it;
                    break;
                }
            }
        }
    }

    if (!LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsDecided()) {
            return 0;
        }
    }

    msg_pool->IncreaseRound(aggregated_mainvote_msg->round + 1);
    CS_LOG_INFO("BA increase round to {}, next_val is {}",
                msg_pool->GetCurrentRound(),
                toHex(next_val).substr(0, 8));
    msg_pool->SwitchToNormalPath();
    if (LegacyVersionBeforeDecisionProof() && msg_pool->IsLegacyCompleted()) {
        return 0;
    }
    auto bval = CreateBvalMessage(msg_pool->GetSeq(),
                                  msg_pool->GetProposer(),
                                  msg_pool->GetCurrentRound(),
                                  next_val,
                                  false,
                                  epoch_number_);
    Signature tmp_sig;
    crypto_helper_->AggregateSign(CalculateBvalDigest(bval, spec_version_), tmp_sig);
    bval->signature = std::move(tmp_sig);
    auto bval_msg = std::make_shared<ssz_types::ConsensusMessage>(bval);
    BroadcastMessage(bval_msg);
    OnRecvBvalMessage(my_id_, bval_msg);

    return 0;
}

int MyBA::OnRecvPromMessage(const NodeId& sender, const MessagePtr msg) {
    auto prom_msg = msg->PromData();
    if (prom_msg == nullptr) {
        return -1;
    }

    if (prom_msg->seq <= stable_seq_.load()) {
        return -10;
    }

    CS_LOG_INFO("receive prom from {} seq: {} proposer: {} hash: {}, round {}",
                toHex(sender).substr(0, 8),
                prom_msg->seq,
                prom_msg->proposer_id.Hex().substr(0, 8),
                prom_msg->hash.Hex().substr(0, 8),
                prom_msg->round);

    Round round = prom_msg->round;
    NodeId nodeid_str = NodeId(prom_msg->proposer_id.Acquire());
    Digest hash_str = Digest(prom_msg->hash.Acquire());

    auto msg_pool = GetMsgPool(ProposalKey(prom_msg->seq, nodeid_str));

    if (CheckCompleted(sender, msg_pool)) {
        reliable_channel_->SendReceipt(sender, msg);
        return -6;
    }

    if (LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsLegacyCompleted() || msg_pool->ExistProm(hash_str, round, sender)) {
            // reliable_channel_->SendReceipt(sender, msg);
            return -3;
        }
    }

    if (msg_pool->ExistProm(hash_str, round, sender)) {
        // reliable_channel_->SendReceipt(sender, msg);
        return -3;
    }
    if (sender != my_id_ && !msg_pool->ExistValidValue(round, hash_str)
        && (!CheckQuorumBalance(prom_msg->prom_proof)
            || !VerifyPhaseOneQC(prom_msg->seq,
                                 nodeid_str,
                                 round,
                                 hash_str,
                                 prom_msg->prom_proof))) {
        CS_LOG_ERROR("verify prom msg failed from {} seq: {}",
                     toHex(sender).substr(0, 8),
                     prom_msg->seq);
        return -4;
    }
    // reliable_channel_->OnRecvMessage(sender, msg);
    msg_pool->AddPromMsg(sender, prom_msg);
    if (!msg_pool->ExistValidValue(round, hash_str)) {
        msg_pool->AddValidValue(round, hash_str);
    }
    if (sender != my_id_) {  // check if I can send Prom/Aux/AggregatedBval as well
        DoPhaseTwoVote(msg_pool,
                       sender,
                       prom_msg->seq,
                       nodeid_str,
                       round,
                       hash_str,
                       prom_msg->prom_proof);
    }
    // prom from old rounds also needs processing, may directly decide
    CheckPhaseTwoVoteStatus(msg_pool, round, hash_str);
    if (msg_pool->ExistAggregatedMainVote(round)) {
        reliable_channel_->SendReceipt(sender, msg);
    }
    return 0;
}

int MyBA::OnRecvAuxMessage(const NodeId& sender, const MessagePtr msg) {
    auto aux_msg = msg->AuxData();
    if (aux_msg == nullptr) {
        return -1;
    }

    if (aux_msg->seq <= stable_seq_.load()) {
        return -10;
    }

    CS_LOG_INFO("receive aux from {} seq: {} proposer: {} hash: {}, round {}",
                toHex(sender).substr(0, 8),
                aux_msg->seq,
                aux_msg->proposer_id.Hex().substr(0, 8),
                aux_msg->hash.Hex().substr(0, 8),
                aux_msg->round);

    Round round = aux_msg->round;
    NodeId nodeid_str = NodeId(aux_msg->proposer_id.Acquire());
    Digest hash_str = Digest(aux_msg->hash.Acquire());
    auto msg_pool = GetMsgPool(ProposalKey(aux_msg->seq, nodeid_str));

    if (round < msg_pool->GetCurrentRound()) {
        reliable_channel_->SendReceipt(sender, msg);
        return -2;
    }

    if (CheckCompleted(sender, msg_pool)) {
        reliable_channel_->SendReceipt(sender, msg);
        return -6;
    }

    if (LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsLegacyCompleted() || msg_pool->ExistAux(hash_str, round, sender)) {
            // reliable_channel_->SendReceipt(sender, msg);
            return -3;
        }
    }

    if (msg_pool->ExistAux(hash_str, round, sender)) {
        // reliable_channel_->SendReceipt(sender, msg);
        return -3;
    }

    if (sender != my_id_ && !msg_pool->ExistValidValue(round, hash_str)
        && (!CheckQuorumBalance(aux_msg->aux_proof)
            || !VerifyPhaseOneQC(aux_msg->seq, nodeid_str, round, hash_str, aux_msg->aux_proof))) {
        CS_LOG_ERROR("verify aux msg failed from {} seq: {}",
                     toHex(sender).substr(0, 8),
                     aux_msg->seq);
        return -4;
    }

    // reliable_channel_->OnRecvMessage(sender, msg);
    msg_pool->AddAuxMsg(sender, aux_msg);
    if (!msg_pool->ExistValidValue(round, hash_str)) {
        msg_pool->AddValidValue(round, hash_str);
    }
    if (msg_pool->GetCurrentRound() == round) {
        if (sender != my_id_) {  // check if I can send Prom/Aux/AggregatedBval as well
            DoPhaseTwoVote(msg_pool,
                           sender,
                           aux_msg->seq,
                           nodeid_str,
                           round,
                           hash_str,
                           aux_msg->aux_proof);
        }
        CheckPhaseTwoVoteStatus(msg_pool, round, hash_str);
        if (msg_pool->ExistAggregatedMainVote(round)) {
            reliable_channel_->SendReceipt(sender, msg);
        }
    }

    return 0;
}

// check if can add valid_value
// if add valid_value, need to check aux and conf
void MyBA::CheckBvalStatus(MyBAMessagePoolPtr msg_pool, const Digest& hash) {
    Round round = msg_pool->GetCurrentRound();
    Seq seq = msg_pool->GetSeq();
    NodeId proposer = msg_pool->GetProposer();
    if (msg_pool->ExistValidValue(round, hash)) {
        return;
    }

    if (msg_pool->GetBvalBalance(hash, round, *peers_balance_) >= quorum_balance_) {
        if (!msg_pool->HavePromAny(round).first && !msg_pool->HaveAuxAny(round).first) {
            ssz_types::AggregateSignature tmp_aggr_sig;
            if (!TryGenerateAggregatedBvalAggrSignature(msg_pool, seq, round, hash, tmp_aggr_sig)) {
                return;
            }
            msg_pool->AddValidValue(round, hash);
            if (hash == ZERO_32_BYTES) {
                msg_pool->SwitchToNormalPath();
            }
            if (msg_pool->HaveBvalOther(round, hash)) {
                auto aux = CreateAuxMessage(seq, proposer, round, hash, std::move(tmp_aggr_sig));
                Signature tmp_sig;
                crypto_helper_->AggregateSign(CalculateAuxDigest(aux), tmp_sig);
                aux->signature = std::move(tmp_sig);
                auto aux_msg = std::make_shared<ssz_types::ConsensusMessage>(aux);
                BroadcastMessage(aux_msg);
                auto& bvals = msg_pool->GetBvalMessages(round, hash);
                for (const auto& bval : bvals) {
                    reliable_channel_->SendReceipt(
                        bval.first,
                        std::make_shared<ssz_types::ConsensusMessage>(bval.second));
                }
                OnRecvAuxMessage(my_id_, aux_msg);
            } else {
                auto prom = CreatePromMessage(seq, proposer, round, hash, std::move(tmp_aggr_sig));
                Signature tmp_sig;
                crypto_helper_->AggregateSign(CalculatePromDigest(prom), tmp_sig);
                prom->signature = std::move(tmp_sig);
                auto prom_msg = std::make_shared<ssz_types::ConsensusMessage>(prom);
                if (proposer == my_id_ || msg_pool->IsNormalPath()) {
                    BroadcastMessage(prom_msg);
                } else {
                    SendMessage(proposer, prom_msg);
                }
                auto& bvals = msg_pool->GetBvalMessages(round, hash);
                for (const auto& bval : bvals) {
                    reliable_channel_->SendReceipt(
                        bval.first,
                        std::make_shared<ssz_types::ConsensusMessage>(bval.second));
                }
                OnRecvPromMessage(my_id_, prom_msg);
            }
        }
        return;
    }

    // endorse if received f+1 bval and have done nothing before
    if (round == 0
        && msg_pool->GetBvalBalance(hash, round, *peers_balance_) > tolerable_faulty_balance_) {
        if (!msg_pool->ExistBval(hash, round, my_id_) && !msg_pool->HaveBvalOther(round, hash)
            && !msg_pool->HavePromAny(round).first && !msg_pool->HaveAuxAny(round).first) {
            endorse_cb_(seq, proposer, hash);
            return;
        }
    }

    // amplify in r>0
    if (round > 0
        && msg_pool->GetBvalBalance(hash, round, *peers_balance_) > tolerable_faulty_balance_) {
        if (!msg_pool->ExistBval(hash, round, my_id_) && !msg_pool->HavePromAny(round).first) {
            auto bval = CreateBvalMessage(seq, proposer, round, hash, false, epoch_number_);
            Signature tmp_sig;
            crypto_helper_->AggregateSign(CalculateBvalDigest(bval, spec_version_), tmp_sig);
            bval->signature = std::move(tmp_sig);
            auto bval_msg = std::make_shared<ssz_types::ConsensusMessage>(bval);
            BroadcastMessage(bval_msg);
            OnRecvBvalMessage(my_id_, bval_msg);
        }
    }
}

void MyBA::CheckPhaseTwoVoteStatus(MyBAMessagePoolPtr msg_pool,
                                   const Round round,
                                   const Digest& hash) {
    Seq seq = msg_pool->GetSeq();
    NodeId proposer = msg_pool->GetProposer();

    std::set<Digest> valid_values;
    msg_pool->GetValidValues(msg_pool->GetCurrentRound(), valid_values);
    std::set<NodeId> participated_nodes;
    for (auto& valid_value : valid_values) {
        const auto& good_aux =
            msg_pool->GetMainGoodNodes(valid_value, round, AggregatedMainVoteType::Aux);
        for (const auto& node_id : good_aux) {
            participated_nodes.insert(node_id);
        }
        const auto& unverified_node_sigs_aux =
            msg_pool->GetMainUnverifiedSigs(valid_value, round, AggregatedMainVoteType::Aux);
        for (const auto& node_sig : unverified_node_sigs_aux) {
            participated_nodes.insert(node_sig.first);
        }
        const auto& good_prom =
            msg_pool->GetMainGoodNodes(valid_value, round, AggregatedMainVoteType::Prom);
        for (const auto& node_id : good_prom) {
            participated_nodes.insert(node_id);
        }
        const auto& unverified_node_sigs_prom =
            msg_pool->GetMainUnverifiedSigs(valid_value, round, AggregatedMainVoteType::Prom);
        for (const auto& node_sig : unverified_node_sigs_prom) {
            participated_nodes.insert(node_sig.first);
        }
    }
    uint64_t cur_balance = 0;
    for (const auto& node_id : participated_nodes) {
        auto itr = peers_balance_->find(node_id);
        if (itr != peers_balance_->end()) {
            cur_balance += itr->second;
        }
    }
    if (cur_balance >= quorum_balance_ && !msg_pool->ExistAggregatedMainVote(round)) {
        std::vector<ssz_types::AggrAuxInfo> tmp_aux_aggr_infos;
        std::vector<ssz_types::AggrPromInfo> tmp_prom_aggr_info;
        if (!TryGenerateMainVoteAggrSignature(msg_pool,
                                              seq,
                                              round,
                                              tmp_aux_aggr_infos,
                                              tmp_prom_aggr_info)) {
            CS_LOG_ERROR("generate aggregated_mainvote failed");
            return;
        }
        auto aggregated_mainvote = CreateAggregatedMainVoteMessage(seq,
                                                                   proposer,
                                                                   round,
                                                                   std::move(tmp_aux_aggr_infos),
                                                                   std::move(tmp_prom_aggr_info));
        auto aggregated_mainvote_msg =
            std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote);
        if (proposer == my_id_) {
            BroadcastMessage(aggregated_mainvote_msg);
        }
        OnRecvAggregatedMainVoteMessage(my_id_, aggregated_mainvote_msg);
    }
    return;
}

bool MyBA::CheckCompleted(const NodeId& sender, MyBAMessagePoolPtr msg_pool) {
    if (LegacyVersionBeforeDecisionProof()) {
        if (!msg_pool->IsLegacyCompleted()) {
            return false;
        }

        AggregatedMainVoteMessagePtr msg;
        if (msg_pool->GetLatestAggregatedMainVoteMsg(msg)) {
            auto aggregated_mainvote_msg = std::make_shared<ssz_types::ConsensusMessage>(msg);
            reliable_channel_->SendMessage(sender, aggregated_mainvote_msg);
            return true;
        }
    } else {
        if (!msg_pool->IsDecided()) {
            return false;
        }

        AggregatedMainVoteMessagePtr msg;
        if (msg_pool->GetDecisionAggregatedMainVoteMsg(msg)) {
            auto aggregated_mainvote_msg = std::make_shared<ssz_types::ConsensusMessage>(msg);
            reliable_channel_->SendMessage(sender, aggregated_mainvote_msg);
            return true;
        }
    }
    return false;
}

bool MyBA::GetCoin(const Round round) {
    // TODO: Currently uses a hard-coded random string as the common coin.
    // Will be replaced with Rondo random beacon (NDSS'25), a VSS-based consistent randomness
    // protocol.
    static std::vector<uint8_t> random_seq = {
        1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0,
        1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 0, 1, 1, 0, 0, 0, 1,
        1, 0, 1, 1, 1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0,
        0, 1, 1, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 1, 1, 1, 0, 1, 0,
        1, 1, 1, 0, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0,
        1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 0, 1, 1, 1,
        0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 1, 1,
        1, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 1};
    if (round < random_seq.size()) {
        return random_seq[round];
    } else {
        return random_seq[round % random_seq.size()];
    }
}

void MyBA::Decide(bool fast_path,
                  const Round round,
                  MyBAMessagePoolPtr msg_pool,
                  const Digest& hash,
                  const AggregatedMainVoteMessagePtr aggregated_mainvote) {
    if (!LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsDecided()) {
            return;
        }
    } else if (msg_pool->IsLegacyCompleted()) {
        return;
    }
    CS_LOG_INFO("MyBA {}-{} decide {} in round {}",
                msg_pool->GetSeq(),
                GetNodeIndex(msg_pool->GetProposer()),
                toHex(hash).substr(0, 8),
                round);
    if (fast_path) {
        if (LegacyVersionBeforeDecisionProof()) {
            msg_pool->Decide(hash, aggregated_mainvote);
            msg_pool->LegacyComplete();
        }
        auto& proms = msg_pool->GetPromMessages(round, hash);
        for (const auto& prom : proms) {
            reliable_channel_->SendReceipt(
                prom.first,
                std::make_shared<ssz_types::ConsensusMessage>(prom.second));
        }
        if (LegacyVersionBeforeDecisionProof()) {
            CETINA_GAUGE_SET(MYBA_DURATION, msg_pool->GetDuration());
            ba_complete_cb_(msg_pool->GetSeq(), msg_pool->GetProposer(), hash);
            return;
        }
    }
    if (LegacyVersionBeforeDecisionProof()) {
        if (msg_pool->IsDecided()) {
            msg_pool->LegacyComplete();
            CETINA_GAUGE_SET(MYBA_DURATION, msg_pool->GetDuration());
            ba_complete_cb_(msg_pool->GetSeq(), msg_pool->GetProposer(), hash);
        } else {
            msg_pool->Decide(hash, aggregated_mainvote);
        }
    } else {
        msg_pool->Decide(hash, aggregated_mainvote);
        CETINA_GAUGE_SET(MYBA_DURATION, msg_pool->GetDuration());
        ba_complete_cb_(msg_pool->GetSeq(), msg_pool->GetProposer(), hash);
    }
}

bool MyBA::VerifyAggregatedMainVote(const AggregatedMainVoteMessagePtr msg) {
    // may have prom0, prom1, aux0, aux1, each signed by distinct sets of nodes.
    // The accumulated balance >= quorum.
    uint64_t cur_balance = 0;
    std::set<NodeId> occurred;
    for (auto& prom_info : msg->aggr_prom_infos.Acquire()) {
        Digest prom_data =
            CalculatePromDigestForAggregatedMainVoteMessage(msg, Digest(prom_info.hash.Acquire()));

        std::vector<NodeId> node_set;
        if (!crypto_helper_->ExtractNodes(std::string(prom_info.aggr_sig.bitmap.Acquire()),
                                          node_set)) {
            CS_LOG_ERROR("extract prom nodes failed");
            return false;
        }
        for (const auto& node_id : node_set) {
            if (!occurred.insert(node_id).second) {  // duplicated
                return false;
            }
            auto itr = peers_balance_->find(node_id);
            if (itr != peers_balance_->end()) {
                cur_balance += itr->second;
            }
        }
        if (!crypto_helper_->AggregateVerify(Signature(prom_info.aggr_sig.aggr_sig.Acquire()),
                                             std::string(prom_info.aggr_sig.bitmap.Acquire()),
                                             prom_data,
                                             node_set.size())) {
            CS_LOG_ERROR("verify aggregate main failed");
            return false;
        }
    }

    for (auto& aux_info : msg->aggr_aux_infos.Acquire()) {
        Digest aux_data =
            CalculateAuxDigestForAggregatedMainVoteMessage(msg, Digest(aux_info.hash.Acquire()));
        std::vector<NodeId> node_set;
        if (!crypto_helper_->ExtractNodes(std::string(aux_info.aggr_sig.bitmap.Acquire()),
                                          node_set)) {
            CS_LOG_ERROR("extract aux nodes failed");
            return false;
        }
        for (const auto& node_id : node_set) {
            if (!occurred.insert(node_id).second) {  // duplicated{
                return false;
            }
            auto itr = peers_balance_->find(node_id);
            if (itr != peers_balance_->end()) {
                cur_balance += itr->second;
            }
        }
        if (!crypto_helper_->AggregateVerify(Signature(aux_info.aggr_sig.aggr_sig.Acquire()),
                                             std::string(aux_info.aggr_sig.bitmap.Acquire()),
                                             aux_data,
                                             node_set.size())) {
            CS_LOG_ERROR("verify aggregate main aux failed");
            return false;
        }
    }

    if (cur_balance < quorum_balance_) {
        return false;
    }

    // When multiple distinct hashes exist, verify each inner proof (bval QC).
    // A single hash is already guaranteed by quorum outer signatures, but with
    // multiple hashes a malicious minority could inject a fake hash with garbage
    // inner proof. Verify prom_proof/aux_proof to prevent this.
    std::set<Digest> received_values;
    for (auto& prom_info : msg->aggr_prom_infos.Acquire()) {
        received_values.emplace(prom_info.hash.Acquire());
    }
    for (auto& aux_info : msg->aggr_aux_infos.Acquire()) {
        received_values.emplace(aux_info.hash.Acquire());
    }
    if (received_values.size() > 1) {
        for (auto& prom_info : msg->aggr_prom_infos.Acquire()) {
            if (!CheckQuorumBalance(prom_info.prom_proof)) {
                CS_LOG_ERROR("prom inner proof (bval QC) lacks quorum balance");
                return false;
            }
            if (!VerifyPhaseOneQC(msg->seq,
                                  NodeId(msg->proposer_id.Acquire()),
                                  msg->round,
                                  Digest(prom_info.hash.Acquire()),
                                  prom_info.prom_proof)) {
                CS_LOG_ERROR("verify prom inner proof (bval QC) failed");
                return false;
            }
        }
        for (auto& aux_info : msg->aggr_aux_infos.Acquire()) {
            if (!CheckQuorumBalance(aux_info.aux_proof)) {
                CS_LOG_ERROR("aux inner proof (bval QC) lacks quorum balance");
                return false;
            }
            if (!VerifyPhaseOneQC(msg->seq,
                                  NodeId(msg->proposer_id.Acquire()),
                                  msg->round,
                                  Digest(aux_info.hash.Acquire()),
                                  aux_info.aux_proof)) {
                CS_LOG_ERROR("verify aux inner proof (bval QC) failed");
                return false;
            }
        }
    }

    return true;
}

bool MyBA::TryGenerateAggregatedBvalAggrSignature(MyBAMessagePoolPtr msg_pool,
                                                  const Seq seq,
                                                  const Round round,
                                                  const Digest& hash,
                                                  ssz_types::AggregateSignature& aggr_sig) {
    const std::set<NodeId>& good_sigs = msg_pool->GetAggregatedBvalGoodNodes(hash, round);
    const std::map<NodeId, Signature>& pending_sigs =
        msg_pool->GetAggregatedBvalUnverifiedSigs(hash, round);
    uint64_t cur_balance = 0;
    for (const NodeId& node_id : good_sigs) {
        auto itr = peers_balance_->find(node_id);
        if (itr != peers_balance_->end()) {
            cur_balance += itr->second;
        }
    }
    for (const auto& id_sig : pending_sigs) {
        auto itr = peers_balance_->find(id_sig.first);
        if (itr != peers_balance_->end()) {
            cur_balance += itr->second;
        }
    }
    if (cur_balance < quorum_balance_) {
        return false;
    }

    std::map<std::string, Signature> bitmap_to_sigs;
    for (auto& sig : pending_sigs) {
        std::string bitmap;
        if (!crypto_helper_->GenerateBitMap({sig.first}, bitmap)) {
            return false;
        }
        bitmap_to_sigs[std::move(bitmap)] = sig.second;
    }
    auto data =
        CalculateBvalDigest(msg_pool->GetBvalMessages(round, hash).begin()->second, spec_version_);

    std::vector<NodeId> good_nodes, bad_nodes;
    std::string output;
    crypto_helper_->OptimisticAggrVerify(bitmap_to_sigs, data, good_nodes, bad_nodes, output);
    msg_pool->GetAggregatedBvalUnverifiedSigs(hash, round).clear();
    if (good_nodes.size() != 0) {
        msg_pool->UpdateAggregatedBvalAggrSignature(round, hash, output, good_nodes);
    }
    const std::set<NodeId>& new_good_nodes = msg_pool->GetAggregatedBvalGoodNodes(hash, round);
    uint64_t new_balance = 0;
    for (const NodeId& node_id : new_good_nodes) {
        auto itr = peers_balance_->find(node_id);
        if (itr != peers_balance_->end()) {
            new_balance += itr->second;
        }
    }
    if (new_balance >= quorum_balance_) {
        std::string bitmap;
        if (!crypto_helper_->GenerateBitMap(msg_pool->GetAggregatedBvalGoodNodes(hash, round),
                                            bitmap)) {
            return false;
        }
        aggr_sig.bitmap = std::move(bitmap);
        aggr_sig.aggr_sig = msg_pool->GetAggregatedBvalAggrSignature(hash, round);
        return true;
    }

    return false;
}

bool MyBA::TryGenerateMainVoteAggrSignature(MyBAMessagePoolPtr msg_pool,
                                            const Seq seq,
                                            const Round round,
                                            std::vector<ssz_types::AggrAuxInfo>& aux_aggr_infos,
                                            std::vector<ssz_types::AggrPromInfo>& prom_aggr_infos) {
    std::set<Digest> valid_values;
    msg_pool->GetValidValues(msg_pool->GetCurrentRound(), valid_values);

    for (auto& valid_value : valid_values) {
        auto tmp_aux_sigs =
            msg_pool->GetMainUnverifiedSigs(valid_value, round, AggregatedMainVoteType::Aux);
        if (tmp_aux_sigs.size() != 0) {
            std::map<std::string, Signature> bitmap_to_aux_sigs;
            for (auto& sig : tmp_aux_sigs) {
                std::string bitmap;
                if (!crypto_helper_->GenerateBitMap({sig.first}, bitmap)) {
                    return false;
                }
                bitmap_to_aux_sigs[std::move(bitmap)] = sig.second;
            }
            auto aux_data =
                CalculateAuxDigest(msg_pool->GetAuxMessages(round, valid_value).begin()->second);
            std::vector<NodeId> aux_good_nodes, aux_bad_nodes;
            std::string aux_output;
            crypto_helper_->OptimisticAggrVerify(bitmap_to_aux_sigs,
                                                 aux_data,
                                                 aux_good_nodes,
                                                 aux_bad_nodes,
                                                 aux_output);
            msg_pool->GetMainUnverifiedSigs(valid_value, round, AggregatedMainVoteType::Aux)
                .clear();
            if (aux_good_nodes.size() != 0) {
                msg_pool->UpdateMainAggrSignature(round,
                                                  valid_value,
                                                  AggregatedMainVoteType::Aux,
                                                  aux_output,
                                                  aux_good_nodes);
            }
        }

        auto tmp_prom_sigs =
            msg_pool->GetMainUnverifiedSigs(valid_value, round, AggregatedMainVoteType::Prom);
        if (tmp_prom_sigs.size() != 0) {
            std::map<std::string, Signature> bitmap_to_prom_sigs;
            for (auto& sig : tmp_prom_sigs) {
                std::string bitmap;
                if (!crypto_helper_->GenerateBitMap({sig.first}, bitmap)) {
                    return false;
                }
                bitmap_to_prom_sigs[std::move(bitmap)] = sig.second;
            }
            auto prom_data =
                CalculatePromDigest(msg_pool->GetPromMessages(round, valid_value).begin()->second);
            std::vector<NodeId> prom_good_nodes, prom_bad_nodes;
            std::string prom_output;
            crypto_helper_->OptimisticAggrVerify(bitmap_to_prom_sigs,
                                                 prom_data,
                                                 prom_good_nodes,
                                                 prom_bad_nodes,
                                                 prom_output);
            msg_pool->GetMainUnverifiedSigs(valid_value, round, AggregatedMainVoteType::Prom)
                .clear();
            if (prom_good_nodes.size() != 0) {
                msg_pool->UpdateMainAggrSignature(round,
                                                  valid_value,
                                                  AggregatedMainVoteType::Prom,
                                                  prom_output,
                                                  prom_good_nodes);
            }
        }
    }

    uint64_t cur_balance = 0;
    for (auto& valid_value : valid_values) {
        const auto& good_aux =
            msg_pool->GetMainGoodNodes(valid_value, round, AggregatedMainVoteType::Aux);
        for (const auto& node_id : good_aux) {
            auto itr = peers_balance_->find(node_id);
            if (itr != peers_balance_->end()) {
                cur_balance += itr->second;
            }
        }
        const auto& good_prom =
            msg_pool->GetMainGoodNodes(valid_value, round, AggregatedMainVoteType::Prom);
        for (const auto& node_id : good_prom) {
            auto itr = peers_balance_->find(node_id);
            if (itr != peers_balance_->end()) {
                cur_balance += itr->second;
            }
        }
    }
    if (cur_balance < quorum_balance_) {
        return false;
    }

    for (auto& valid_value : valid_values) {
        if (msg_pool->GetAuxMessages(round, valid_value).size() == 0) {
            continue;
        }
        ssz_types::AggrAuxInfo tmp_aux_aggr_info;
        tmp_aux_aggr_info.hash = valid_value;
        std::string bitmap;
        if (!crypto_helper_->GenerateBitMap(
                msg_pool->GetMainGoodNodes(valid_value, round, AggregatedMainVoteType::Aux),
                bitmap)) {
            return false;
        }
        tmp_aux_aggr_info.aggr_sig.bitmap = std::move(bitmap);
        tmp_aux_aggr_info.aggr_sig.aggr_sig =
            msg_pool->GetMainAggrSignature(valid_value, round, AggregatedMainVoteType::Aux);
        ssz_types::AggregateSignature aux_proof;
        aux_proof.bitmap = std::string(msg_pool->GetAuxMessages(round, valid_value)
                                           .begin()
                                           ->second->aux_proof.bitmap.Acquire());
        aux_proof.aggr_sig = std::string(msg_pool->GetAuxMessages(round, valid_value)
                                             .begin()
                                             ->second->aux_proof.aggr_sig.Acquire());
        tmp_aux_aggr_info.aux_proof = std::move(aux_proof);
        aux_aggr_infos.emplace_back(std::move(tmp_aux_aggr_info));
    }

    for (auto& valid_value : valid_values) {
        if (msg_pool->GetPromCount(valid_value, round) == 0) {
            continue;
        }
        ssz_types::AggrPromInfo tmp_prom_aggr_info;
        tmp_prom_aggr_info.hash = valid_value;
        std::string bitmap;
        if (!crypto_helper_->GenerateBitMap(
                msg_pool->GetMainGoodNodes(valid_value, round, AggregatedMainVoteType::Prom),
                bitmap)) {
            return false;
        }
        tmp_prom_aggr_info.aggr_sig.bitmap = std::move(bitmap);
        tmp_prom_aggr_info.aggr_sig.aggr_sig =
            msg_pool->GetMainAggrSignature(valid_value, round, AggregatedMainVoteType::Prom);
        ssz_types::AggregateSignature prom_proof;
        prom_proof.bitmap = std::string(msg_pool->GetPromMessages(round, valid_value)
                                            .begin()
                                            ->second->prom_proof.bitmap.Acquire());
        prom_proof.aggr_sig = std::string(msg_pool->GetPromMessages(round, valid_value)
                                              .begin()
                                              ->second->prom_proof.aggr_sig.Acquire());
        tmp_prom_aggr_info.prom_proof = std::move(prom_proof);
        prom_aggr_infos.emplace_back(std::move(tmp_prom_aggr_info));
    }

    return true;
}

uint16_t MyBA::GetNodeIndex(const NodeId& node_id) {
    return reliable_channel_->GetNodeIndex(node_id);
}

MyBAMessagePoolPtr MyBA::GetMsgPool(const ProposalKey& key) {
    auto iter = ba_msg_pool_.find(key);
    if (iter != ba_msg_pool_.end()) {
        return iter->second;
    }
    MyBAMessagePoolPtr msg_pool =
        std::make_shared<MyBAMessagePool>(my_id_, key.seq_, key.proposer_id_, crypto_helper_);
    ba_msg_pool_.emplace(key, msg_pool);
    return msg_pool;
}

// uint16_t MyBA::Threshold_f() {
//     return f_ + 1;
// }

// uint16_t MyBA::Threshold_quorum() {
//     return n_ - f_;
// }

bool MyBA::CheckQuorumBalance(const ssz_types::AggregateSignature& aggr_signature) {
    std::vector<NodeId> node_set;
    if (!crypto_helper_->ExtractNodes(std::string(aggr_signature.bitmap.Acquire()), node_set)) {
        CS_LOG_ERROR("extract nodes failed");
        return false;
    }
    uint64_t cur_balance = 0;
    for (const auto& node_id : node_set) {
        auto itr = peers_balance_->find(node_id);
        if (itr != peers_balance_->end()) {
            cur_balance += itr->second;
        }
    }
    if (cur_balance < quorum_balance_) {
        CS_LOG_ERROR("quorum not reached in proof");
        return false;
    }
    return true;
}

bool MyBA::VerifyPhaseOneQC(const Seq seq,
                            const NodeId& proposer_id,
                            const Round round,
                            const Digest& hash,
                            const ssz_types::AggregateSignature& aggr_signature) {
    auto bval_digest =
        CalculateBvalDigest(seq, proposer_id, round, hash, epoch_number_, spec_version_);
    if (!crypto_helper_->AggregateVerify(std::string(aggr_signature.aggr_sig.Acquire()),
                                         std::string(aggr_signature.bitmap.Acquire()),
                                         bval_digest,
                                         1 /* at least 1 signature*/)) {
        CS_LOG_ERROR("aggregate verify failed");
        return false;
    }
    return true;
}

void MyBA::DoPhaseTwoVote(MyBAMessagePoolPtr msg_pool,
                          const NodeId& sender,
                          const Seq seq,
                          const NodeId& proposer_id,
                          const Round round,
                          const Digest& hash,
                          const ssz_types::AggregateSignature& aggr_signature) {
    if (!msg_pool->HavePromAny(round).first && !msg_pool->HaveAuxAny(round).first) {
        if (hash == ZERO_32_BYTES) {
            msg_pool->SwitchToNormalPath();
        }

        ssz_types::AggregateSignature tmp_aggr_sig;
        tmp_aggr_sig.bitmap = std::string(aggr_signature.bitmap.Acquire());
        tmp_aggr_sig.aggr_sig = std::string(aggr_signature.aggr_sig.Acquire());
        if (msg_pool->HaveBvalOther(round, hash)) {
            auto aux = CreateAuxMessage(seq, proposer_id, round, hash, std::move(tmp_aggr_sig));
            Signature tmp_sig;
            crypto_helper_->AggregateSign(CalculateAuxDigest(aux), tmp_sig);
            aux->signature = std::move(tmp_sig);
            auto aux_msg = std::make_shared<ssz_types::ConsensusMessage>(aux);
            if (proposer_id == sender && !msg_pool->IsNormalPath()) {
                SendMessage(proposer_id, aux_msg);
            } else {
                BroadcastMessage(aux_msg);
            }
            auto& bvals = msg_pool->GetBvalMessages(round, hash);
            for (const auto& bval : bvals) {
                reliable_channel_->SendReceipt(
                    bval.first,
                    std::make_shared<ssz_types::ConsensusMessage>(bval.second));
            }
            OnRecvAuxMessage(my_id_, aux_msg);
        } else {
            auto prom = CreatePromMessage(seq, proposer_id, round, hash, std::move(tmp_aggr_sig));
            Signature tmp_sig;
            crypto_helper_->AggregateSign(CalculatePromDigest(prom), tmp_sig);
            prom->signature = std::move(tmp_sig);
            auto prom_msg = std::make_shared<ssz_types::ConsensusMessage>(prom);
            if (proposer_id == sender && !msg_pool->IsNormalPath()) {
                SendMessage(proposer_id, prom_msg);
            } else {
                BroadcastMessage(prom_msg);
            }
            auto& bvals = msg_pool->GetBvalMessages(round, hash);
            for (const auto& bval : bvals) {
                reliable_channel_->SendReceipt(
                    bval.first,
                    std::make_shared<ssz_types::ConsensusMessage>(bval.second));
            }
            OnRecvPromMessage(my_id_, prom_msg);
        }
    }
}

bool MyBA::LegacyVersionBeforeDecisionProof() {
    return spec_version_ < CONSENSUS_VERSION_DECIDE_WITH_CERT;
}

}  // namespace consensus_spec
