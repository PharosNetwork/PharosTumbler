// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once
#include "consensus/protocol/reliable_channel.h"

namespace consensus_spec {
class MockReliableChannel : public ReliableChannelBase {
  public:
    MockReliableChannel() = default;
    ~MockReliableChannel() = default;

    void Start() override {}

    void Stop() override {}

    bool ConfigureDB(const bool need_persist_message,
                     const std::shared_ptr<wal::WalLocalStorage> db) override {
        return true;
    }

    void SetNodes(const std::shared_ptr<IdToPublicKeyMap> new_nodes) override {}

    void GarbageCollection(const Seq seq) override {}

    void SendMessage(const NodeId& node_id, const MessagePtr msg) override {
        last_sent_msg_ = msg;
        sent_msg_count_++;
    }

    void BroadcastMessage(const MessagePtr msg) override {
        last_broadcast_msg_ = msg;
        broadcast_msg_count_++;
        broadcast_msgs_.push_back(msg);
    }

    void SaveOutgoingMessage(const MessagePtr msg) override {}

    void SendReceipt(const NodeId& node_id, const MessagePtr msg) override {}

    void OnRecvMessage(const NodeId& sender, const MessagePtr msg) override {}

    void ResendMessages(const Seq seq) override {}

    void LoadMessages(
        const Seq low_seq,
        const Seq high_seq,
        const uint32_t epoch_number,
        std::vector<ValMessagePtr>& vals,
        std::unordered_map<NodeId, Signature>& skips,
        std::unordered_map<NodeId, std::vector<std::pair<NodeId, MessagePtr>>>& myba_msgs,
        std::map<Seq, std::set<NodeId>>& endorsed,
        std::vector<PassMessagePtr>& pass_msgs) override {}

    void PersistRawBlock(const Seq seq,
                         std::map<wal::EntryType, std::vector<bytes>>&& consensus_result) override {
    }

    void ResetResendTimer() override {}

    NodeId GetNodeId(const uint16_t node_index) override {
        return NodeId();
    }

    uint16_t GetNodeIndex(const NodeId& node_id) override {
        return 0;
    }

    uint32_t GetSentMsgCount() {
        return sent_msg_count_;
    }

    MessagePtr GetLastSentMsg() {
        return last_sent_msg_;
    }

    uint32_t GetBroadcastMsgCount() {
        return broadcast_msg_count_;
    }

    MessagePtr GetLastBroadcastMsg() {
        return last_broadcast_msg_;
    }

    std::vector<MessagePtr>& GetBroadcastMsgs() {
        return broadcast_msgs_;
    }

    void ClearBroadcastMsgs() {
        broadcast_msgs_.clear();
    }

  private:
    uint32_t sent_msg_count_ = 0;
    MessagePtr last_sent_msg_ = nullptr;
    uint32_t broadcast_msg_count_ = 0;
    std::vector<MessagePtr> broadcast_msgs_;
    MessagePtr last_broadcast_msg_ = nullptr;
};
}  // namespace consensus_spec