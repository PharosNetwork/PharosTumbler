// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "consensus/libraries/common/conversion.h"
#include "consensus/protocol/mytumbler_message_types.h"

namespace consensus_spec {
using consensus_spec::ssz_types::AggregateSignature;
class MyTumblerMessageTypesTest : public testing::Test {
    static void SetUpTestCase() {
        Digester::digest_ = [](const std::string& data) {
            return "";
        };
    }
};

TEST_F(MyTumblerMessageTypesTest, MessageGenericInfoTest) {
    MessageGenericInfo m1(0, "000000", 2, 0, Digest());
    MessageGenericInfo m2(2, "000000", 0, 0, Digest());
    MessageGenericInfo m3(0, "000000", 0, 0, Digest());
    MessageGenericInfo m4(0, "000000", 2, 0, Digest());
    EXPECT_TRUE(m1 < m2);
    EXPECT_TRUE(!(m1 < m3));
    EXPECT_TRUE(m3 < m4);
    EXPECT_TRUE(m1 == m4);
}

TEST_F(MyTumblerMessageTypesTest, SendQueueIndexTest) {
    SendQueueIndex k1(ZERO_32_BYTES);
    k1.msg_info_.seq_ = 1;
    SendQueueIndex k2(ZERO_32_BYTES);
    k2.msg_info_.seq_ = 2;
    EXPECT_TRUE(k1 < k2);
    k2.msg_info_.seq_ = 1;
    EXPECT_TRUE(k1 == k2);
    k2.msg_info_.round_ = 10;
    EXPECT_TRUE(k1 < k2);
}

TEST_F(MyTumblerMessageTypesTest, PersistentKeyTest) {
    PersistentKey k1(ZERO_32_BYTES);
    k1.msg_info_.seq_ = 1;
    PersistentKey k2(ZERO_32_BYTES);
    k2.msg_info_.seq_ = 2;
    EXPECT_TRUE(k1 < k2);
    k2.msg_info_.seq_ = 1;
    EXPECT_TRUE(k1 == k2);
    k2.msg_info_.round_ = 10;
    EXPECT_TRUE(k1 < k2);
}

TEST_F(MyTumblerMessageTypesTest, GetMessageInfoTest) {
    NodeId id1(32, '1');
    {
        MessageGenericInfo info;
        NodeId proposer = id1;
        std::string proposal = "payload";
        ValMessagePtr val = CreateValMessage(1, proposer, 123456, bytes(proposal.begin(), proposal.end()));
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(val), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::ValMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 0);
        EXPECT_EQ(info.proposer_id_, proposer);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        BvalMessagePtr bval = CreateBvalMessage(1, id1, 1, "1");
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(bval), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::BvalMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 1);
        EXPECT_EQ(info.proposer_id_, id1);
        EXPECT_TRUE(info.hash_ == "1");
    }

    {
        MessageGenericInfo info;
        PromMessagePtr prom = CreatePromMessage(1, id1, 1, "1", AggregateSignature());
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(prom), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::PromMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 1);
        EXPECT_EQ(info.proposer_id_, id1);
    }

    {
        MessageGenericInfo info;
        AuxMessagePtr aux = CreateAuxMessage(1, id1, 1, "1", AggregateSignature());
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(aux), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::AuxMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 1);
        EXPECT_EQ(info.proposer_id_, id1);
    }

    {
        MessageGenericInfo info;
        PassMessagePtr pass = CreatePassMessage(1, std::vector<ssz::ByteVector<32>>());
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(pass), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::PassMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 0);
        EXPECT_EQ(info.proposer_id_, std::string(32, '0'));
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        SkipMessagePtr skip = CreateSkipMessage(1, id1, Signature());
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(skip), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::SkipMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 0);
        EXPECT_EQ(info.proposer_id_, id1);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        std::vector<ssz_types::Signature> signatures;
        ForwardSkipMessagePtr forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(forward_skip), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::ForwardSkipMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 0);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        AggregatedBvalMessagePtr aggregated_bval = CreateAggregatedBvalMessage(1, id1, 2, "hash1", AggregateSignature());
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(aggregated_bval), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::AggregatedBvalMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 2);
        EXPECT_EQ(info.proposer_id_, id1);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        AggregatedBvalReceiptMessagePtr aggregated_bval_receipt = CreateAggregatedBvalReceiptMessage(1, id1, 2);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(aggregated_bval_receipt), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::AggregatedBvalReceiptMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 2);
        EXPECT_EQ(info.proposer_id_, id1);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        AggregatedMainVoteReceiptMessagePtr aggregated_mainvote_receipt = CreateAggregatedMainVoteReceiptMessage(1, id1, 2);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote_receipt), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::AggregatedMainVoteReceiptMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 2);
        EXPECT_EQ(info.proposer_id_, id1);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        RequestProposalMessagePtr request_proposal = CreateRequestProposalMessage(1, std::vector<ssz_types::ProposalRequestKey>());
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(request_proposal), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::RequestProposalMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 0);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        MessageGenericInfo info;
        NodeId proposer = id1;
        std::string proposal = "payload";
        ValMessagePtr val = CreateValMessage(1, proposer, 123456, bytes(proposal.begin(), proposal.end()));
        std::vector<ValMessagePtr> vals;
        vals.push_back(val);
        ResponseProposalMessagePtr response_proposal = CreateResponseProposalMessage(vals);
        GetMessageInfo(std::make_shared<ssz_types::ConsensusMessage>(response_proposal), info);
        EXPECT_EQ(info.msg_type_, ConsensusMessageType::ResponseProposalMessage);
        EXPECT_EQ(info.seq_, 1);
        EXPECT_EQ(info.round_, 0);
        EXPECT_TRUE(info.hash_ == Digest());
    }

    {
        // Test default case - invalid message (nullptr)
        MessageGenericInfo info;
        bool result = GetMessageInfo(nullptr, info);
        EXPECT_FALSE(result);
    }

    {
        // Test default case - message with invalid data (nullptr internal data)
        MessageGenericInfo info;
        auto invalid_msg = std::make_shared<ssz_types::ConsensusMessage>();
        bool result = GetMessageInfo(invalid_msg, info);
        EXPECT_FALSE(result);
    }
}

}  // namespace consensus_spec