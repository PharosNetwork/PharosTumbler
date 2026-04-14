// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <memory>
#include <map>
#include <list>
#include <vector>
#include <unordered_map>

#include "consensus/common/consensus_common.h"
#include "consensus/common/metrics.h"
#include "consensus/libraries/common/type_define.h"
#include "consensus/libraries/common/conversion.h"
#include "consensus/libraries/thread/worker.h"
#include "consensus/libraries/wal/wal.h"
// #include "consensus/protocol/mytumbler_types.h"
#include "consensus/protocol/mytumbler_message_types.h"

namespace consensus_spec {

using SendMsg = std::function<void(const NodeId& node_id, const MessagePtr msg)>;
using BroadcastMsg = std::function<void(const MessagePtr msg)>;

class ReliableChannelBase {
  public:
    ReliableChannelBase() = default;
    virtual ~ReliableChannelBase() = default;

    virtual void Start() = 0;
    virtual void Stop() = 0;
    virtual bool ConfigureDB(const bool need_persist_message,
                             const std::shared_ptr<wal::WalLocalStorage> db) = 0;
    virtual void SendMessage(const NodeId& node_id, const MessagePtr msg) = 0;
    virtual void BroadcastMessage(const MessagePtr msg) = 0;
    virtual void SaveOutgoingMessage(const MessagePtr msg) = 0;
    virtual void SendReceipt(const NodeId& node_id, const MessagePtr msg) = 0;
    virtual void OnRecvMessage(const NodeId& sender, const MessagePtr msg) = 0;
    virtual void ResendMessages(const Seq seq) = 0;
    virtual void LoadMessages(
        const Seq low_seq,
        const Seq high_seq,
        const uint32_t epoch_number,
        std::vector<ValMessagePtr>& vals,
        std::unordered_map<NodeId, Signature>& skips,
        std::unordered_map<NodeId, std::vector<std::pair<NodeId, MessagePtr>>>& myba_msgs,
        std::map<Seq, std::set<NodeId>>& endorsed,
        std::vector<PassMessagePtr>& pass_msgs) = 0;
    virtual void SetNodes(const std::shared_ptr<IdToPublicKeyMap> new_nodes) = 0;
    virtual void GarbageCollection(const Seq seq) = 0;
    virtual void PersistRawBlock(
        const Seq seq,
        std::map<wal::EntryType, std::vector<bytes>>&& consensus_result) = 0;
    virtual void ResetResendTimer() = 0;
    virtual NodeId GetNodeId(const uint16_t node_index) = 0;
    virtual uint16_t GetNodeIndex(const NodeId& node_id) = 0;
};

class ReliableChannel : public ReliableChannelBase, public Worker {
  public:
    ReliableChannel(const uint16_t spec_version,
                    const NodeId& my_id,
                    const uint64_t resend_timeout,
                    const uint64_t resend_interval,
                    SendMsg send,
                    BroadcastMsg broadcast);
    ~ReliableChannel();

    bool ConfigureDB(const bool need_persist_message,
                     const std::shared_ptr<wal::WalLocalStorage> db) override;
    void SendMessage(const NodeId& node_id, const MessagePtr msg) override;
    void BroadcastMessage(const MessagePtr msg) override;
    void SaveOutgoingMessage(const MessagePtr msg) override;
    void SendReceipt(const NodeId& node_id, const MessagePtr msg) override;
    void OnRecvMessage(const NodeId& sender, const MessagePtr msg) override;
    void ResendMessages(const Seq seq) override;
    void LoadMessages(
        const Seq low_seq,
        const Seq high_seq,
        const uint32_t epoch_number,
        std::vector<ValMessagePtr>& vals,
        std::unordered_map<NodeId, Signature>& skips,
        std::unordered_map<NodeId, std::vector<std::pair<NodeId, MessagePtr>>>& myba_msgs,
        std::map<Seq, std::set<NodeId>>& endorsed,
        std::vector<PassMessagePtr>& pass_msgs) override;
    void SetNodes(const std::shared_ptr<IdToPublicKeyMap> new_nodes) override;
    void GarbageCollection(const Seq seq) override;
    void PersistRawBlock(const Seq seq,
                         std::map<wal::EntryType, std::vector<bytes>>&& consensus_result) override;
    void ResetResendTimer() override;
    NodeId GetNodeId(const uint16_t node_index) override;
    uint16_t GetNodeIndex(const NodeId& node_id) override;

  public:
    void Start() override;
    void Stop() override;

  private:
    void DoWork() override;

    void DoSendMessage(const NodeId& node_id, const MessagePtr msg);
    void DoBroadcastMessage(const MessagePtr msg);
    void DoRecvMessage(const NodeId& sender, const MessagePtr msg);
    void DoSendReceipt(const NodeId& node_id, const MessagePtr msg);
    void DoGarbageCollection(const Seq seq);
    void DoSaveMessage(const MessagePtr msg);
    void DoPersistRawBlock(const Seq seq,
                           std::map<wal::EntryType, std::vector<bytes>>&& consensus_result);
    void DoResetResendTimer();

    void LoadMessages(const Seq low_seq, const Seq high_seq, std::vector<MessagePtr>& msgs);
    void Flush();
    void FlushPersistent();
    void Resend();
    bytes EncodeMessage(const MessagePtr msg);
    MessagePtr DecodeMessage(bytes&& data);

  private:
    uint16_t spec_version_{0};
    NodeId my_id_;

    std::unordered_map<NodeId, uint16_t> node_id_to_index_;
    std::unordered_map<uint16_t, NodeId> node_index_to_id_;

    static constexpr size_t kMaxQueueSize = 10000;

    // msgs waiting to be sent
    std::map<SendQueueIndex, MessagePtr> send_queue_;
    std::list<MessagePtr> broadcast_queue_;
    // msgs sent without ack
    std::map<SendQueueIndex, MessagePtr> resend_queue_;
    // receipts waiting to be sent
    std::list<std::pair<NodeId, MessagePtr>> receipt_queue_;
    std::map<Seq, std::map<wal::EntryType, std::vector<bytes>>> persistent_entry_queue_;

    uint64_t resend_timer_{500};
    uint64_t resend_timeout_{20000};
    uint64_t resend_interval_{5000};
    uint64_t current_loop_time_{0};

    bool need_persist_message_{true};
    std::shared_ptr<wal::WalLocalStorage> db_;
    SendMsg send_;
    BroadcastMsg broadcast_;
};

using ReliableChannelBasePtr = std::shared_ptr<ReliableChannelBase>;
using ReliableChannelPtr = std::shared_ptr<ReliableChannel>;

}  // namespace consensus_spec
