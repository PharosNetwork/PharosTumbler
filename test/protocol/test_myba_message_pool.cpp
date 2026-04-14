// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "consensus/libraries/common/conversion.h"
#include "consensus/protocol/myba_message_pool.h"

namespace consensus_spec {
using consensus_spec::ssz_types::AggregateSignature;

class MyBAMessagePoolTest : public testing::Test {
  public:
    MyBAMessagePoolTest() {
        auto crypto_helper = std::make_shared<CryptoHelper>();
        auto ecc_signer = std::make_shared<ECCSigner>(
            [this](const std::string& data, std::string& sig) {
                sig = "signature";
            },
            [this](const std::string& data,
                   const std::string& sig,
                   const consensus_spec::NodeId& from) {
                if (sig == "signature") {
                    return true;
                }
                return false;
            });
        auto aggr_sign = [](const std::string&, std::string&) {};
        auto aggr_verify = [](const std::string&, const std::string&,
                             const std::string&, const size_t) { return true; };
        auto aggr_signature = [](std::string&, const std::string&) { return true; };
        auto opt_aggr_verify = [](const std::map<std::string, std::string>&,
                                 const std::string&,
                                 std::vector<std::string>&,
                                 std::vector<std::string>&,
                                 std::string&) { return true; };
        auto gen_bitmap = [](const std::set<NodeId>&) { return std::string(); };
        auto extract_nodes = [](const std::string&) { return std::vector<NodeId>(); };
        auto dummy_aggregator = std::make_shared<BLSAggregator>(
            aggr_sign, aggr_verify, aggr_signature, opt_aggr_verify, gen_bitmap, extract_nodes);
        crypto_helper->InitCryptoFunc(
            [this](const std::string& data) {
                return "digest";
            },
            ecc_signer,
            dummy_aggregator);
        id0_ = std::string(32, '0');
        id1_ = std::string(32, '1');
        msg_pool_ = std::make_shared<MyBAMessagePool>(id0_, 1, id1_, crypto_helper);
        EXPECT_EQ(msg_pool_->current_round_, 0);
        EXPECT_TRUE(!msg_pool_->decided_);
    }

    void DoBvalTest() {
        EXPECT_TRUE(msg_pool_->bval_.empty());
        BvalMessagePtr bval = CreateBvalMessage(1, id1_, 1, "1");
        msg_pool_->AddBvalMsg(id0_, bval);
        EXPECT_TRUE(msg_pool_->ExistBval(Digest("1"), 1, id0_));
        EXPECT_TRUE(!msg_pool_->ExistBval(Digest("1"), 0, id0_));
        EXPECT_TRUE(!msg_pool_->ExistBval(ZERO_32_BYTES, 1, id0_));
        EXPECT_TRUE(!msg_pool_->ExistBval(Digest("1"), 1, id1_));
        EXPECT_EQ(msg_pool_->bval_[1][Digest("1")].size(), 1);
        EXPECT_TRUE(!msg_pool_->HaveBvalOther(1, Digest("1")));
        EXPECT_TRUE(msg_pool_->HaveBvalOther(1, ZERO_32_BYTES));
    }

    void DoPromTest() {
        EXPECT_TRUE(msg_pool_->prom_.empty());
        PromMessagePtr prom = CreatePromMessage(1, id1_, 1, "1", AggregateSignature());
        msg_pool_->AddPromMsg(id0_, prom);
        EXPECT_TRUE(msg_pool_->ExistProm(Digest("1"), 1, id0_));
        EXPECT_TRUE(!msg_pool_->ExistProm(ZERO_32_BYTES, 1, id0_));
        EXPECT_TRUE(!msg_pool_->ExistProm(Digest("1"), 0, id0_));
        EXPECT_TRUE(!msg_pool_->ExistProm(Digest("1"), 1, id1_));
        EXPECT_EQ(msg_pool_->GetPromCount(Digest("1"), 1), 1);
        EXPECT_TRUE(msg_pool_->HavePromAny(1).first);
        EXPECT_TRUE(*prom == *(msg_pool_->HavePromAny(1).second));
        EXPECT_TRUE(!msg_pool_->HavePromAny(0).first);
    }

    void DoAuxTest() {
        EXPECT_TRUE(msg_pool_->aux_.empty());
        AuxMessagePtr aux = CreateAuxMessage(1, id1_, 1, "1", AggregateSignature());
        msg_pool_->AddAuxMsg(id0_, aux);
        EXPECT_TRUE(msg_pool_->ExistAux(Digest("1"), 1, id0_));
        EXPECT_TRUE(!msg_pool_->ExistAux(ZERO_32_BYTES, 1, id0_));
        EXPECT_TRUE(!msg_pool_->ExistAux(Digest("1"), 1, NodeId(id1_)));
        EXPECT_TRUE(!msg_pool_->ExistAux(Digest("1"), 0, id0_));
        EXPECT_EQ(msg_pool_->aux_[1][Digest("1")].size(), 1);
        EXPECT_TRUE(msg_pool_->HaveAuxAny(1).first);
        EXPECT_TRUE(*aux == *(msg_pool_->HaveAuxAny(1).second));
        EXPECT_TRUE(!msg_pool_->HaveAuxAny(0).first);
    }

    MyBAMessagePoolPtr msg_pool_;
    NodeId id0_;
    NodeId id1_;
};

TEST_F(MyBAMessagePoolTest, BvalTest) {
    DoBvalTest();
}

TEST_F(MyBAMessagePoolTest, PromTest) {
    DoPromTest();
}

TEST_F(MyBAMessagePoolTest, AuxTest) {
    DoAuxTest();
}

}  // namespace consensus_spec
