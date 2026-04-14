// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/protocol/reliable_channel.h"

#include <pamir/cetina/cetina.h>
#include "consensus/common/consensus_common.h"
#include "consensus/libraries/log/consensus_log.h"
#include "consensus/libraries/utils/time_utils.h"
#include "consensus/schema/consensus.ssz.h"

namespace consensus_spec {

ReliableChannel::ReliableChannel(const uint16_t spec_version,
                                 const NodeId& my_id,
                                 const uint64_t resend_timeout,
                                 const uint64_t resend_interval,
                                 SendMsg send,
                                 BroadcastMsg broadcast)
        : Worker("reliable_channel", 1),
          spec_version_(spec_version),
          my_id_(my_id),
          resend_timeout_(resend_timeout),
          resend_interval_(resend_interval),
          send_(send),
          broadcast_(broadcast) {}

ReliableChannel::~ReliableChannel() {
    StopWorking();
}

bool ReliableChannel::ConfigureDB(const bool need_persist_message,
                                  const std::shared_ptr<wal::WalLocalStorage> db) {
    need_persist_message_ = need_persist_message;  // always save, not necessarily load
    if (db) {
        db_ = db;
        return true;
    } else {
        CS_LOG_FATAL("Invalid db!");
        return false;
    }
}

void ReliableChannel::SendMessage(const NodeId& node_id, const MessagePtr msg) {
    if (node_id == my_id_ || msg == nullptr) {
        return;
    }
    PushTask([this, node_id, msg]() {
        DoSendMessage(node_id, msg);
    });
}

void ReliableChannel::BroadcastMessage(const MessagePtr msg) {
    if (msg == nullptr) {
        return;
    }
    PushTask([this, msg]() {
        DoBroadcastMessage(msg);
    });
}

void ReliableChannel::SaveOutgoingMessage(const MessagePtr msg) {
    if (msg == nullptr) {
        return;
    }
    PushTask([this, msg]() {
        DoSaveMessage(msg);
    });
}

void ReliableChannel::SendReceipt(const NodeId& node_id, const MessagePtr msg) {
    if (node_id == my_id_ || !msg) {
        return;
    }
    PushTask([this, node_id, msg]() {
        DoSendReceipt(node_id, msg);
    });
}

void ReliableChannel::OnRecvMessage(const NodeId& sender, const MessagePtr msg) {
    if (sender == my_id_) {
        return;
    }

    if (msg == nullptr) {
        CS_LOG_ERROR("nullptr msg from sender {}", toHex(sender).substr(0, 8));
        return;
    }

    PushTask([this, sender, msg]() {
        DoRecvMessage(sender, msg);
    });
}

void ReliableChannel::LoadMessages(const Seq low_seq,
                                   const Seq high_seq,
                                   std::vector<MessagePtr>& msgs) {
    // load all msgs among [low_seq, high_seq]
    msgs.clear();
    if (!need_persist_message_) {
        return;
    }
    for (auto seq = low_seq; seq <= high_seq; ++seq) {
        std::vector<std::string> entries;
        db_->GetEntries(seq, wal::WAL_ENTRY_TYPE_MESSAGE, entries);
        CS_LOG_DEBUG("load {} msgs, seq {}", entries.size(), seq);
        for (const auto& entry : entries) {
            auto msg = DecodeMessage(toBytes(entry));
            if (msg == nullptr) {
                CS_LOG_ERROR("decode msg failed");
                continue;
            }
            switch (msg->Index()) {
                case ConsensusMessageType::ValMessage:
                case ConsensusMessageType::AggregatedBvalMessage:
                case ConsensusMessageType::PromMessage:
                case ConsensusMessageType::AuxMessage:
                case ConsensusMessageType::AggregatedMainVoteMessage:
                case ConsensusMessageType::SkipMessage:
                case ConsensusMessageType::PassMessage: {
                    msgs.push_back(msg);
                    break;
                }
                default: {
                    CS_LOG_ERROR("invalid msg type {}", msg->Index());
                    break;
                }
            }
        }  // end for loaded entries
    }  // end for all sequences
}

void ReliableChannel::ResendMessages(const Seq seq) {
    std::vector<MessagePtr> msgs;
    LoadMessages(seq, seq, msgs);
    for (const auto& msg : msgs) {
        if (msg->Index() == ConsensusMessageType::ValMessage) {
            NodeId proposer(msg->ValData()->proposer_id.Acquire());
            if (proposer != my_id_) {
                SendReceipt(proposer, msg);
            } else {
                BroadcastMessage(msg);
            }
        } else {
            BroadcastMessage(msg);
        }
    }
}

void ReliableChannel::LoadMessages(
    const Seq low_seq,
    const Seq high_seq,
    const uint32_t epoch_number,
    std::vector<ValMessagePtr>& vals,
    std::unordered_map<NodeId, Signature>& skips,
    std::unordered_map<NodeId, std::vector<std::pair<NodeId, MessagePtr>>>& myba_msgs,
    std::map<Seq, std::set<NodeId>>& endorsed,
    std::vector<PassMessagePtr>& pass_msgs) {
    vals.clear();
    skips.clear();
    myba_msgs.clear();
    pass_msgs.clear();

    std::vector<MessagePtr> msgs;
    LoadMessages(low_seq, high_seq, msgs);
    // better move this loop to mytumbler engine
    for (const auto& msg : msgs) {
        if (msg->Index() == ConsensusMessageType::ValMessage
            && epoch_number != msg->ValData()->epoch_number) {
            CS_LOG_INFO("load val from epoch {} not match current epoch {}",
                        msg->ValData()->epoch_number,
                        epoch_number);
            continue;
        }
        MessageGenericInfo msg_info;
        GetMessageInfo(msg, msg_info);
        if (msg->Index() == ConsensusMessageType::ValMessage && msg_info.proposer_id_ != my_id_) {
            SendReceipt(msg_info.proposer_id_, msg);
        } else {
            BroadcastMessage(msg);
        }

        if (msg->Index() == ConsensusMessageType::ValMessage) {
            vals.emplace_back(msg->ValData());
            endorsed[msg_info.seq_].emplace(msg_info.proposer_id_);
        } else if (msg->Index() == ConsensusMessageType::SkipMessage) {
            skips.emplace(msg_info.proposer_id_, Signature(msg->SkipData()->signature.Acquire()));
        } else if (msg->Index() == ConsensusMessageType::PassMessage) {
            pass_msgs.emplace_back(msg->PassData());
        } else {
            myba_msgs[msg_info.proposer_id_].emplace_back(std::make_pair(my_id_, msg));
        }
    }
}

void ReliableChannel::SetNodes(const std::shared_ptr<IdToPublicKeyMap> nodes) {
    uint16_t index = 1;
    for (const auto& node : *nodes) {
        node_id_to_index_[node.first] = index;
        node_index_to_id_[index] = node.first;
        CS_LOG_INFO("node id: {} index: {}", toHex(node.first).substr(0, 8), index);
        ++index;
    }
}

void ReliableChannel::Start() {
    StartWorking();
}

void ReliableChannel::Stop() {
    StopWorking();
}

void ReliableChannel::GarbageCollection(const Seq seq) {
    PushTask([this, seq]() {
        DoGarbageCollection(seq);
    });
}

void ReliableChannel::PersistRawBlock(
    const Seq seq,
    std::map<wal::EntryType, std::vector<bytes>>&& consensus_result) {
    PushTask([this, seq, consensus_result = std::move(consensus_result)]() mutable {
        DoPersistRawBlock(seq, std::move(consensus_result));
    });
}

void ReliableChannel::DoPersistRawBlock(
    const Seq seq,
    std::map<wal::EntryType, std::vector<bytes>>&& consensus_result) {
    persistent_entry_queue_[seq].merge(consensus_result);
    // Try to persist immediately so subsequent receipts/messages are less likely to be sent
    // before WAL append completes. If persistence fails, queue remains for retry.
    FlushPersistent();
}

void ReliableChannel::ResetResendTimer() {
    PushTask([this]() {
        DoResetResendTimer();
    });
}

void ReliableChannel::DoResetResendTimer() {
    resend_timer_ = resend_timeout_;
    current_loop_time_ = 0;
}

void ReliableChannel::DoWork() {
    Flush();

    if (current_loop_time_++ == resend_timer_) {
        resend_timer_ = resend_interval_;
        Resend();
        current_loop_time_ = 0;
    }
}

void ReliableChannel::DoSendMessage(const NodeId& node_id, const MessagePtr msg) {
    SendQueueIndex index(node_id);
    if (!GetMessageInfo(msg, index.msg_info_)) {
        CS_LOG_ERROR("decode msg failed");
        return;
    }

    if (persistent_entry_queue_.empty()) {
        send_(node_id, msg);
        if (resend_queue_.size() >= kMaxQueueSize) {
            CS_LOG_WARN("resend_queue_ full, dropping oldest message");
            resend_queue_.erase(resend_queue_.begin());
        }
        resend_queue_.emplace(index, msg);
    } else {
        if (send_queue_.size() >= kMaxQueueSize) {
            CS_LOG_WARN("send_queue_ full, dropping oldest message");
            send_queue_.erase(send_queue_.begin());
        }
        send_queue_.emplace(index, msg);
    }
}

void ReliableChannel::DoBroadcastMessage(const MessagePtr msg) {
    if (persistent_entry_queue_.empty()) {
        for (const auto& iter : node_id_to_index_) {
            if (iter.first == my_id_) {
                continue;
            }
            SendQueueIndex index(iter.first);
            if (!GetMessageInfo(msg, index.msg_info_)) {
                CS_LOG_ERROR("decode msg failed");
                return;
            }
            resend_queue_.emplace(index, msg);
        }
        broadcast_(msg);
    } else {
        if (broadcast_queue_.size() >= kMaxQueueSize) {
            CS_LOG_WARN("broadcast_queue_ full, dropping oldest message");
            broadcast_queue_.pop_front();
        }
        broadcast_queue_.emplace_back(msg);
    }
}

void ReliableChannel::DoRecvMessage(const NodeId& sender, const MessagePtr msg) {
    // handle receipt msg
    if ((msg->Index() >= ConsensusMessageType::ValReceiptMessage
         && msg->Index() <= ConsensusMessageType::AggregatedMainVoteReceiptMessage)
        || msg->Index() == ConsensusMessageType::ForwardSkipReceiptMessage) {
        SendQueueIndex index(sender);
        GetMessageInfo(msg, index.msg_info_);
        switch (msg->Index()) {
            case ConsensusMessageType::ValReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::ValMessage;
                break;
            }
            case ConsensusMessageType::BvalReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::BvalMessage;
                break;
            }
            case ConsensusMessageType::AggregatedBvalReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::AggregatedBvalMessage;
                break;
            }
            case ConsensusMessageType::PromReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::PromMessage;
                break;
            }
            case ConsensusMessageType::AuxReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::AuxMessage;
                break;
            }
            case ConsensusMessageType::AggregatedMainVoteReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::AggregatedMainVoteMessage;
                break;
            }
            case ConsensusMessageType::PassReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::PassMessage;
                break;
            }
            case ConsensusMessageType::SkipReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::SkipMessage;
                break;
            }
            case ConsensusMessageType::ForwardSkipReceiptMessage: {
                index.msg_info_.msg_type_ = ConsensusMessageType::ForwardSkipMessage;
                break;
            }
            default: {
                CS_LOG_ERROR("invalid msg type: {}", msg->Index());
                return;
            }
        }
        auto it = resend_queue_.find(index);
        if (it != resend_queue_.end()) {
            resend_queue_.erase(it);
        }
        return;
    }

    DoSaveMessage(msg);
    DoSendReceipt(sender, msg);
}

void ReliableChannel::DoSendReceipt(const NodeId& node_id, const MessagePtr msg) {
    MessageGenericInfo msg_info;
    GetMessageInfo(msg, msg_info);
    MessagePtr receipt = std::make_shared<ssz_types::ConsensusMessage>();
    switch (msg->Index()) {
        case ConsensusMessageType::ValMessage: {
            *receipt = CreateValReceiptMessage(msg_info.seq_,
                                               msg_info.proposer_id_,
                                               msg->ValData()->epoch_number);
            break;
        }
        case ConsensusMessageType::BvalMessage: {
            *receipt = CreateBvalReceiptMessage(msg_info.seq_,
                                                msg_info.proposer_id_,
                                                msg_info.round_,
                                                msg_info.hash_,
                                                msg->BvalData()->epoch_number);
            break;
        }
        case ConsensusMessageType::AggregatedBvalMessage: {
            *receipt = CreateAggregatedBvalReceiptMessage(msg_info.seq_,
                                                          msg_info.proposer_id_,
                                                          msg_info.round_);
            break;
        }
        case ConsensusMessageType::PromMessage: {
            *receipt =
                CreatePromReceiptMessage(msg_info.seq_, msg_info.proposer_id_, msg_info.round_);
            break;
        }
        case ConsensusMessageType::AuxMessage: {
            *receipt =
                CreateAuxReceiptMessage(msg_info.seq_, msg_info.proposer_id_, msg_info.round_);
            break;
        }
        case ConsensusMessageType::AggregatedMainVoteMessage: {
            *receipt = CreateAggregatedMainVoteReceiptMessage(msg_info.seq_,
                                                              msg_info.proposer_id_,
                                                              msg_info.round_);
            break;
        }
        case ConsensusMessageType::PassMessage: {
            *receipt = CreatePassReceiptMessage(msg_info.seq_);
            break;
        }
        case ConsensusMessageType::SkipMessage: {
            *receipt = CreateSkipReceiptMessage(msg_info.seq_, msg_info.proposer_id_);
            break;
        }
        case ConsensusMessageType::ForwardSkipMessage: {
            *receipt = CreateForwardSkipReceiptMessage(msg_info.seq_);
            break;
        }
        default: {
            CS_LOG_ERROR("invalid msg type: {}", msg->Index());
            return;
        }
    }

    if (receipt) {
        if (persistent_entry_queue_.empty()) {
            send_(node_id, receipt);
        } else {
            receipt_queue_.emplace_back(std::make_pair(node_id, receipt));
        }
    }
}

void ReliableChannel::DoGarbageCollection(const Seq seq) {
    auto start_time = time_utils::GetSteadyTimePoint();
    for (auto it = send_queue_.begin(); it != send_queue_.end();) {
        if (it->first.msg_info_.seq_ > seq) {
            break;
        }
        it = send_queue_.erase(it);
    }
    for (auto it = resend_queue_.begin(); it != resend_queue_.end();) {
        if (it->first.msg_info_.seq_ > seq) {
            break;
        }
        it = resend_queue_.erase(it);
    }

    persistent_entry_queue_.erase(persistent_entry_queue_.begin(),
                                  persistent_entry_queue_.upper_bound(seq));
    CETINA_GAUGE_SET(RC_GC_DURAION, time_utils::GetDuration(start_time));
}

void ReliableChannel::DoSaveMessage(const MessagePtr msg) {
    /*
    ValMessage
    AggregatedBvalMessage
    PromMessage
    PromCertificateMessage_DEPRECATED
    AuxMessage
    AggregatedMainVoteMessage
    SkipMessage
    PassMessage
    */
    MessageGenericInfo info;
    if (!GetMessageInfo(msg, info)) {
        CS_LOG_ERROR("get msg info failed, type {}", msg->Index());
        return;
    }
    bytes value = EncodeMessage(msg);
    persistent_entry_queue_[info.seq_][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].emplace_back(
        std::move(value));
}

void ReliableChannel::Flush() {
    FlushPersistent();

    auto start_time = time_utils::GetSteadyTimePoint();
    for (auto& item : send_queue_) {
        send_(item.first.receiver_id_, item.second);
        if (resend_queue_.size() >= kMaxQueueSize) {
            CS_LOG_WARN("resend_queue_ full, dropping oldest message");
            resend_queue_.erase(resend_queue_.begin());
        }
        resend_queue_.emplace(item.first, item.second);
    }
    for (auto& msg : broadcast_queue_) {
        broadcast_(msg);
        for (const auto& iter : node_id_to_index_) {
            if (iter.first == my_id_) {
                continue;
            }
            SendQueueIndex index(iter.first);
            if (!GetMessageInfo(msg, index.msg_info_)) {
                CS_LOG_ERROR("decode msg failed");
                return;
            }
            if (resend_queue_.size() >= kMaxQueueSize) {
                CS_LOG_WARN("resend_queue_ full, dropping oldest message");
                resend_queue_.erase(resend_queue_.begin());
            }
            resend_queue_.emplace(index, msg);
        }
    }
    CETINA_GAUGE_SET(RC_BROADCAST_QUEUE_SIZE, broadcast_queue_.size());
    CETINA_GAUGE_SET(RC_SEND_QUEUE_SIZE, send_queue_.size());
    broadcast_queue_.clear();
    send_queue_.clear();
    CETINA_GAUGE_SET(RC_SEND_DURATION, time_utils::GetDuration(start_time));

    start_time = time_utils::GetSteadyTimePoint();
    for (auto& item : receipt_queue_) {
        send_(item.first, item.second);
    }
    CETINA_GAUGE_SET(RC_RECEIPT_DURATION, time_utils::GetDuration(start_time));
    CETINA_GAUGE_SET(RC_RECEIPT_QUEUE_SIZE, receipt_queue_.size());
    receipt_queue_.clear();
}

void ReliableChannel::FlushPersistent() {
    if (persistent_entry_queue_.empty()) {
        return;
    }

    auto start_time = time_utils::GetSteadyTimePoint();
    size_t size = 0;
    for (auto iter = persistent_entry_queue_.begin(); iter != persistent_entry_queue_.end();
         ++iter) {
        size += iter->second.size();
        db_->Append(iter->first, iter->second);
    }
    persistent_entry_queue_.clear();

    auto duration = time_utils::GetDuration(start_time);
    CS_LOG_DEBUG("persist duration {}", duration);
    CETINA_HISTOGRAM_OBSERVE(RC_PERSIST_DURATION, duration, 1 / 1e4, 2, 20);
    CETINA_GAUGE_SET(RC_PERSIST_QUEUE_SIZE, size);
}

void ReliableChannel::Resend() {
    auto start_time = time_utils::GetSteadyTimePoint();
    for (auto& item : resend_queue_) {
        send_(item.first.receiver_id_, item.second);
    }
    CETINA_GAUGE_SET(RC_RESEND_QUEUE_SIZE, resend_queue_.size());
    CETINA_GAUGE_SET(RC_SEND_DURATION, time_utils::GetDuration(start_time));
}

// guarantee index is valid
NodeId ReliableChannel::GetNodeId(const uint16_t index) {
    return node_index_to_id_[index];
}

// guarantee node_id is valid
uint16_t ReliableChannel::GetNodeIndex(const NodeId& node_id) {
    return node_id_to_index_[node_id];
}

bytes ReliableChannel::EncodeMessage(const MessagePtr msg) {
    size_t raw_data_size =
        ssz::SSZObjectView<ssz_types::ConsensusMessage>::PackedSize(*msg, spec_version_);
    bytes raw_data(raw_data_size);
    ssz::Buffer buf{raw_data.data(), raw_data_size};
    auto msg_view = ssz::SSZObjectView<ssz_types::ConsensusMessage>(buf);
    msg_view.Pack(*msg, spec_version_);
    return raw_data;
}

MessagePtr ReliableChannel::DecodeMessage(bytes&& data) {
    ssz::Buffer buffer{&data[0], data.size()};
    ssz::SSZObjectView<ssz_types::ConsensusMessage> msg_view{buffer};
    if (msg_view.Verify(spec_version_) != ssz::SSZError::OK) {
        CS_LOG_ERROR("invalid msg");
        return nullptr;
    }
    MessagePtr msg_data = std::make_shared<consensus_spec::ssz_types::ConsensusMessage>();
    if (msg_view.Unpack(*msg_data, spec_version_) != ssz::SSZError::OK) {
        CS_LOG_ERROR("invalid msg");
        return nullptr;
    }
    return msg_data;
}

}  // namespace consensus_spec
