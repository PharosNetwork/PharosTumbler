// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "consensus/protocol/reliable_channel.h"
#include "mytumbler_engine_fixture.h"

namespace consensus_spec {
using consensus_spec::ssz_types::AggregateSignature;

class ReliableChannelTest : public testing::Test {
  public:
    ReliableChannelTest() : id1_(std::string(31, '0') + "1"), id2_(std::string(31, '0') + "2") {}

    static void SetUpTestCase() {
        Digester::digest_ = [](const std::string& data) {
            return "";
        };
    }

    void SendMessage(const NodeId& node_id, const MessagePtr msg) {
        sended_[node_id].push_back(msg);
    }

    void BroadcastMessage(const MessagePtr msg) {
        for (auto it : sended_) {
            it.second.push_back(msg);
        }
    }

  public:
    std::map<NodeId, std::vector<MessagePtr>> sended_;
    std::string id1_;  // "000...1"
    std::string id2_;  // "000...2"

  public:
    ReliableChannelPtr CreateReliableChannel() {
        auto reliable_channel = std::make_shared<ReliableChannel>(
            0,
            id1_,
            20000,
            5000,
            [this](const NodeId& node_id, const MessagePtr msg) {
                this->SendMessage(node_id, msg);
            },
            [this](const MessagePtr msg) {
                this->BroadcastMessage(msg);
            });

        auto nodes = std::make_shared<IdToPublicKeyMap>();
        for (uint32_t i = 0; i < 4; ++i) {
            nodes->emplace(NodeId(std::string(31, '0') + std::to_string(i + 1)), PublicKey());
        }
        reliable_channel->SetNodes(nodes);
        // create
        auto db_path = "log/test_reliable_channel";
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::make_shared<wal::WalLocalStorage>(option);
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);
        wal = std::make_shared<wal::WalLocalStorage>(option);
        return reliable_channel->ConfigureDB(true, wal) ? reliable_channel : nullptr;
    }

    void SendMessageTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        auto self = id1_;
        auto dst = id2_;
        bytes proposal;
        auto msg = CreateValMessage(1, self, 0, proposal);

        // nil msg or send to myself
        rc->SendMessage(self, std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        rc->SendMessage(dst, nullptr);
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());

        // send directly
        rc->DoSendMessage(dst, nullptr);
        EXPECT_TRUE(rc->resend_queue_.empty());
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_TRUE(!rc->resend_queue_.empty());
        EXPECT_TRUE(rc->send_queue_.empty());
    }

    void BroadcastMessageTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        auto self = id1_;
        bytes proposal;
        auto msg = CreateValMessage(1, self, 0, proposal);

        // nil msg
        rc->BroadcastMessage(nullptr);
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());

        // broadcast directly
        rc->DoBroadcastMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->resend_queue_.size(), 3);
        EXPECT_TRUE(rc->send_queue_.empty());
    }

    void SaveOutgoingMessageTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // nil msg
        rc->SaveOutgoingMessage(nullptr);
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // save data directly
        auto dst = id2_;
        bytes proposal;
        auto msg = CreateValMessage(1, dst, 0, proposal);
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
    }

    void SendReceiptTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        auto self = id1_;
        auto dst = id2_;
        bytes proposal;
        auto msg = CreateValMessage(1, dst, 0, proposal);

        // send to myself
        rc->SendReceipt(self, std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());

        // send receipt directly
        rc->DoSendReceipt(dst, std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(sended_[dst].size() != 0);
    }

    void OnRecvMessageTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        auto self = id1_;
        auto dst = id2_;
        bytes proposal;
        // receive my msg
        rc->OnRecvMessage(self, nullptr);
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // receive msg
        auto val = CreateValMessage(1, dst, 0, proposal);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(val));
        EXPECT_EQ(rc->receipt_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1].size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
        auto bval = CreateBvalMessage(1, dst, 0, ZERO_32_BYTES);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(bval));
        EXPECT_EQ(rc->receipt_queue_.size(), 2);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 2);
        auto aux = CreateAuxMessage(1, dst, 0, ZERO_32_BYTES, AggregateSignature());
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(aux));
        EXPECT_EQ(rc->receipt_queue_.size(), 3);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 3);
        ssz_types::Signature tmp_signature;
        tmp_signature.node_id = "sender";
        tmp_signature.signature = Signature("signature");
        auto prom = CreatePromMessage(1, dst, 0, ZERO_32_BYTES, AggregateSignature());
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(prom));
        EXPECT_EQ(rc->receipt_queue_.size(), 4);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 4);

        auto pass = CreatePassMessage(1, std::vector<ssz::ByteVector<32>>());
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(pass));
        EXPECT_EQ(rc->receipt_queue_.size(), 5);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 5);
        auto skip = CreateSkipMessage(1, dst, Signature());
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(skip));
        EXPECT_EQ(rc->receipt_queue_.size(), 6);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 6);
        std::vector<ssz_types::Signature> signatures;
        auto forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(forward_skip));
        EXPECT_EQ(rc->receipt_queue_.size(), 7);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 7);

        // receive receipt msg
        val = CreateValMessage(1, self, 0, proposal);
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(val));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        MessageGenericInfo msg_info;
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(val), msg_info);
        auto r1 = CreateValReceiptMessage(msg_info.seq_, msg_info.proposer_id_);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r1));
        EXPECT_TRUE(rc->resend_queue_.empty());

        auto digest = ZERO_32_BYTES;
        bval = CreateBvalMessage(1, self, 0, digest);
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(bval));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(bval), msg_info);
        auto r2 = CreateBvalReceiptMessage(msg_info.seq_,
                                           msg_info.proposer_id_,
                                           msg_info.round_,
                                           msg_info.hash_,
                                           0);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r2));
        EXPECT_TRUE(rc->resend_queue_.empty());

        prom = CreatePromMessage(1, self, 0, digest, AggregateSignature());
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(prom));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(prom), msg_info);
        auto r3 = CreatePromReceiptMessage(msg_info.seq_, msg_info.proposer_id_, msg_info.round_);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r3));
        EXPECT_TRUE(rc->resend_queue_.empty());

        aux = CreateAuxMessage(1, self, 0, digest, AggregateSignature());
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(aux));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(aux), msg_info);
        auto r4 = CreateAuxReceiptMessage(msg_info.seq_, msg_info.proposer_id_, msg_info.round_);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r4));
        EXPECT_TRUE(rc->resend_queue_.empty());

        std::vector<ssz::ByteList<0, 32>> tmp_valid_values_2{};
        rc->Flush();

        pass = CreatePassMessage(1, std::vector<ssz::ByteVector<32>>());
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(pass));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(pass), msg_info);
        auto r7 = CreatePassReceiptMessage(msg_info.seq_);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r7));
        EXPECT_TRUE(rc->resend_queue_.empty());

        skip = CreateSkipMessage(1, dst, Signature());
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(skip));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(skip), msg_info);
        auto r8 = CreateSkipReceiptMessage(msg_info.seq_, msg_info.proposer_id_);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r8));
        EXPECT_TRUE(rc->resend_queue_.empty());

        std::vector<ssz_types::Signature> signatures2;
        forward_skip = CreateForwardSkipMessage(1, std::move(signatures2));
        rc->DoSendMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(forward_skip));
        rc->Flush();
        EXPECT_TRUE(rc->resend_queue_.size() == 1);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(forward_skip), msg_info);
        auto r9 = CreateForwardSkipReceiptMessage(msg_info.seq_);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(r9));
        EXPECT_TRUE(rc->resend_queue_.empty());
    }

    void LoadMessageFromDbTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        auto dst = id2_;
        bytes proposal;
        auto msg = CreateValMessage(1, dst, 0, proposal, 999);  // v<7 does not care about epoch
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        auto bval = CreateBvalMessage(1, dst, 0, "1");
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(bval));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // reset wal storage
        rc->db_ = std::make_shared<wal::WalLocalStorage>(rc->db_->option_);

        std::vector<ValMessagePtr> vals;
        std::vector<PassMessagePtr> pass_msgs;
        std::unordered_map<NodeId, Signature> skips;
        std::unordered_map<NodeId, std::vector<std::pair<NodeId, MessagePtr>>> myba_msgs;
        std::map<Seq, std::set<NodeId>> endorsed;
        rc->LoadMessages(1, 1, 0, vals, skips, myba_msgs, endorsed, pass_msgs);
        EXPECT_EQ(vals.size(), 1);
        EXPECT_EQ(vals[0]->epoch_number, 0);  // epoch 999 is not encoded
        EXPECT_EQ(endorsed.size(), 1);
        EXPECT_EQ(endorsed.begin()->second.size(), 1);
        EXPECT_EQ(myba_msgs.size(), 0);
        EXPECT_EQ(pass_msgs.size(), 0);

        // new version load old message
        rc->spec_version_ = 7;
        endorsed.clear();
        rc->LoadMessages(1, 1, 0, vals, skips, myba_msgs, endorsed, pass_msgs);
        EXPECT_EQ(vals.size(), 0);
        EXPECT_EQ(endorsed.size(), 0);
        EXPECT_EQ(myba_msgs.size(), 0);
        EXPECT_EQ(pass_msgs.size(), 0);

        // new version load new version message
        uint32_t expected_epoch_number = 3;
        msg = CreateValMessage(1, dst, 0, proposal, expected_epoch_number);
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        rc->db_ = std::make_shared<wal::WalLocalStorage>(rc->db_->option_);
        rc->LoadMessages(1, 1, expected_epoch_number, vals, skips, myba_msgs, endorsed, pass_msgs);
        EXPECT_EQ(vals.size(), 1);
        EXPECT_EQ(endorsed.size(), 1);
        EXPECT_EQ(endorsed.begin()->second.size(), 1);
        EXPECT_EQ(myba_msgs.size(), 0);
        EXPECT_EQ(pass_msgs.size(), 0);

        // new version load old epoch message
        Seq seq = 2;
        msg = CreateValMessage(seq, dst, 0, proposal, expected_epoch_number - 1);
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[seq][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(),
                  1);
        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        rc->db_ = std::make_shared<wal::WalLocalStorage>(rc->db_->option_);
        endorsed.clear();
        rc->LoadMessages(seq,
                         seq,
                         expected_epoch_number,
                         vals,
                         skips,
                         myba_msgs,
                         endorsed,
                         pass_msgs);
        EXPECT_EQ(vals.size(), 0);
        EXPECT_EQ(endorsed.size(), 0);
        EXPECT_EQ(myba_msgs.size(), 0);
        EXPECT_EQ(pass_msgs.size(), 0);
    }

    void GarbageCollectionTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        NodeId self(id1_);
        NodeId dst(id2_);
        bytes proposal;
        auto msg = CreateValMessage(1, dst, 0, proposal);

        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_TRUE(!rc->persistent_entry_queue_.empty());
        EXPECT_TRUE(!rc->receipt_queue_.empty());

        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());

        rc->DoGarbageCollection(1);

        auto new_msg = CreateValMessage(2, dst, 0, proposal);
        rc->DoRecvMessage(dst, std::make_shared<ssz_types::ConsensusMessage>(new_msg));
        rc->Flush();
        rc->DoGarbageCollection(1);
        rc->DoGarbageCollection(2);

        SendQueueIndex index1(1, self, dst, 0, 0, Digest());
        rc->send_queue_.emplace(index1, std::make_shared<ssz_types::ConsensusMessage>(msg));
        rc->resend_queue_.emplace(index1, std::make_shared<ssz_types::ConsensusMessage>(msg));
        SendQueueIndex index2(1, self, self, 0, 0, Digest());
        rc->send_queue_.emplace(index2, std::make_shared<ssz_types::ConsensusMessage>(msg));
        rc->resend_queue_.emplace(index2, std::make_shared<ssz_types::ConsensusMessage>(msg));
        SendQueueIndex index3(2, self, self, 0, 0, Digest());
        rc->send_queue_.emplace(index3, std::make_shared<ssz_types::ConsensusMessage>(msg));
        rc->resend_queue_.emplace(index3, std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->send_queue_.size(), 3);
        EXPECT_EQ(rc->resend_queue_.size(), 3);
        rc->DoGarbageCollection(1);
        EXPECT_EQ(rc->send_queue_.size(), 1);
        EXPECT_EQ(rc->resend_queue_.size(), 1);
    }

    void ResendMessagesTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // other's Val will not be resent
        auto dst = id2_;
        bytes proposal;
        auto msg = CreateValMessage(1, dst, 0, proposal);
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
        rc->Flush();  // will not into receipt_queue_, broadcast_queue_, send_queue_
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // own message
        msg = CreateValMessage(2, rc->my_id_, 0, proposal);
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(msg));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[2][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);
        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        // Bval msg will not be loaded
        auto bval = CreateBvalMessage(1, dst, 0, "1");
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(bval));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 1);
        EXPECT_EQ(rc->persistent_entry_queue_[1][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);

        // Pass msg will be reload and resent
        auto pass = CreatePassMessage(2, std::vector<ssz::ByteVector<32>>());
        rc->DoSaveMessage(std::make_shared<ssz_types::ConsensusMessage>(pass));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 2);
        EXPECT_EQ(rc->persistent_entry_queue_[2][wal::EntryType::WAL_ENTRY_TYPE_MESSAGE].size(), 1);

        rc->ResendMessages(1);
        // register callable but not yet running
        EXPECT_EQ(rc->receipt_queue_.size(), 0);
        EXPECT_EQ(rc->resend_queue_.size(), 0);

        rc->StartWorking();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        EXPECT_EQ(rc->persistent_entry_queue_.size(), 0);
        rc->ResendMessages(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        EXPECT_EQ(rc->receipt_queue_.size(), 0);
        EXPECT_EQ(rc->resend_queue_.size(), 0);

        rc->Flush();
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());
        rc->ResendMessages(2);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        EXPECT_EQ(rc->resend_queue_.size(), 6);
    }

    void ResendTest(ReliableChannelPtr rc) {
        EXPECT_TRUE(rc->send_queue_.empty());
        EXPECT_TRUE(rc->resend_queue_.empty());
        EXPECT_TRUE(rc->receipt_queue_.empty());
        EXPECT_TRUE(rc->persistent_entry_queue_.empty());

        EXPECT_EQ(rc->resend_timer_, 500);
        EXPECT_EQ(rc->resend_timeout_, 20000);
        EXPECT_EQ(rc->resend_interval_, 5000);

        auto msg = CreateValMessage(1, id1_, 0, bytes());
        rc->DoSendMessage(id2_, std::make_shared<ssz_types::ConsensusMessage>(msg));
        rc->current_loop_time_ = 500;
        rc->DoWork();
        EXPECT_EQ(rc->resend_timer_, 5000);
        EXPECT_EQ(rc->current_loop_time_, 0);

        rc->DoResetResendTimer();
        EXPECT_EQ(rc->resend_timer_, 20000);
    }
};

TEST_F(ReliableChannelTest, SendMessageTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    SendMessageTest(reliable_channel);
}

TEST_F(ReliableChannelTest, BroadcastMessageTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    BroadcastMessageTest(reliable_channel);
}

TEST_F(ReliableChannelTest, SaveOutgoinfMessageTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    SaveOutgoingMessageTest(reliable_channel);
}

TEST_F(ReliableChannelTest, SendReceiptTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    SendReceiptTest(reliable_channel);
}

TEST_F(ReliableChannelTest, OnRecvMessageTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    OnRecvMessageTest(reliable_channel);
}

TEST_F(ReliableChannelTest, LoadMessageFromDbTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    LoadMessageFromDbTest(reliable_channel);
}

TEST_F(ReliableChannelTest, GarbageCollectionTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    GarbageCollectionTest(reliable_channel);
}

TEST_F(ReliableChannelTest, ResendMessagesTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    ResendMessagesTest(reliable_channel);
}

TEST_F(ReliableChannelTest, ResendTest) {
    auto reliable_channel = CreateReliableChannel();
    EXPECT_TRUE(reliable_channel != nullptr);
    ResendTest(reliable_channel);
}

TEST_F(ReliableChannelTest, DoPersistRawBlockFlushesImmediately) {
    auto rc = CreateReliableChannel();
    EXPECT_TRUE(rc != nullptr);
    EXPECT_TRUE(rc->persistent_entry_queue_.empty());

    // Prepare consensus result data
    std::map<wal::EntryType, std::vector<bytes>> consensus_result;
    consensus_result[wal::EntryType::WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(
        bytes({'t', 'e', 's', 't'}));

    // Call DoPersistRawBlock directly — after the fix, this should flush immediately
    rc->DoPersistRawBlock(1, std::move(consensus_result));

    // persistent_entry_queue_ should be empty because FlushPersistent was called
    EXPECT_TRUE(rc->persistent_entry_queue_.empty());
}
}  // namespace consensus_spec
