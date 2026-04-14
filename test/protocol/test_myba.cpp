// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "mytumbler_engine_fixture.h"

#include "consensus/protocol/myba.h"

namespace consensus_spec {
using consensus_spec::ssz_types::AggregateSignature;

#define NOWTIME                                                                                  \
    static_cast<long long unsigned int>(std::chrono::duration_cast<std::chrono::milliseconds>(   \
                                            std::chrono::system_clock::now().time_since_epoch()) \
                                            .count())

class MyBATest : public testing::Test, mytumbler_engine_fixture {
  public:
    MyBATest() : mytumbler_engine_fixture() {
        ReliableChannelBasePtr rc = std::make_shared<MockReliableChannel>();
        myba_no_aggrbval_ = std::make_shared<MyBA>(
            7,
            6,  // epoch
            my_id_,
            4,
            3,  // tolerable_faulty_balance out of 10 total balance
            7,  // quorum_balance out of 10 total balance
            0,  // stable seq
            engine_->crypto_helper_,
            peer_pubkey_map_,
            peer_balance_map_,
            [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
                endorsed_[seq][proposer] = hash;
            },
            [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {
            },
            [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
                myba_completed_[seq][proposer] = hash;
            },
            rc);
    }

    MyBAPtr myba_no_aggrbval_;
    std::map<Seq, std::map<NodeId, Digest>> endorsed_;
    std::map<Seq, std::map<NodeId, Digest>> myba_completed_;

  public:
    void DoOnRecvBvalMessageTest() {
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[1], nullptr), -1);
        MessagePtr consensus_msg = std::make_shared<ssz_types::ConsensusMessage>();
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[1], consensus_msg), -99);

        BvalMessagePtr msg = nullptr;
        Digest hash;

        // outdated msg
        msg = CreateBvalMessage(0, peers_[1], 0, Digest());
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[1],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -10);

        msg->seq = 1;
        NodeId nodeid_str = NodeId(msg->proposer_id.Acquire());
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(ProposalKey(msg->seq, nodeid_str));
        ++(msg_pool->current_round_);
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -3);
        --(msg_pool->current_round_);

        // CheckFastPathCompleted true
        msg_pool->legacy_completed_ = true;
        auto aggregated_mainvote =
            CreateAggregatedMainVoteMessage(1,
                                            peers_[1],
                                            0,
                                            std::vector<ssz_types::AggrAuxInfo>(),
                                            std::vector<ssz_types::AggrPromInfo>());
        msg_pool->aggregated_mainvote_msg_[0] = aggregated_mainvote;
        msg_pool->decision_certificate_ = aggregated_mainvote;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -6);
        msg_pool->legacy_completed_ = false;
        msg_pool->aggregated_mainvote_msg_.clear();

        // IsCompleted
        msg_pool->legacy_completed_ = true;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);
        msg_pool->legacy_completed_ = false;
        // HavePromAny
        msg_pool->prom_[0][Digest()][msg_pool->my_id_] = Signature();
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);
        msg_pool->prom_.clear();
        // HaveAuxAny
        auto tmp_aux_msg =
            CreateAuxMessage(1, peers_[1], 0, Digest(), ssz_types::AggregateSignature());
        msg_pool->aux_[0][Digest()][msg_pool->my_id_] = tmp_aux_msg;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);
        msg_pool->aux_.clear();

        msg->signature =
            MockAggregateSign(2, CalculateBvalDigest(msg, myba_no_aggrbval_->spec_version_));
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            0);
        EXPECT_FALSE(msg_pool->ExistValidValue(0, Digest()));

        // duplicated
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);

        // recv f+1 bval
        BvalMessagePtr b2 = CreateBvalMessage(1, peers_[1], 0, Digest());
        b2->signature =
            MockAggregateSign(3, CalculateBvalDigest(b2, myba_no_aggrbval_->spec_version_));
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(b2)),
            0);
        EXPECT_TRUE(endorsed_[1][peers_[1]] == Digest(msg->hash.Acquire()));
        EXPECT_FALSE(msg_pool->ExistValidValue(0, Digest()));

        // recv 2f+1 bval
        BvalMessagePtr b_self = CreateBvalMessage(1, peers_[1], 0, Digest());
        Signature tmp_sig;
        myba_no_aggrbval_->crypto_helper_->AggregateSign(
            CalculateBvalDigest(b_self, myba_no_aggrbval_->spec_version_),
            tmp_sig);
        b_self->signature = tmp_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[0],
                                             std::make_shared<ssz_types::ConsensusMessage>(b_self)),
            0);
        EXPECT_TRUE(msg_pool->ExistValidValue(0, Digest()));

        // amplify in r>0
        auto proposal = ProposalKey(1, peers_[1]);
        myba_no_aggrbval_->GetMsgPool(proposal)->IncreaseRound(
            myba_no_aggrbval_->GetMsgPool(proposal)->GetCurrentRound() + 1);
        EXPECT_EQ(myba_no_aggrbval_->GetMsgPool(proposal)->GetCurrentRound(), 1);
        BvalMessagePtr b3 =
            CreateBvalMessage(1, peers_[1], 1, Digest(), false, myba_no_aggrbval_->epoch_number_);
        b3->signature =
            MockAggregateSign(1, CalculateBvalDigest(b3, myba_no_aggrbval_->spec_version_));
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[1],
                                             std::make_shared<ssz_types::ConsensusMessage>(b3)),
            0);
        EXPECT_TRUE(!myba_no_aggrbval_->GetMsgPool(proposal)->ExistBval(hash, 1, peers_[0]));
        b3->signature =
            MockAggregateSign(2, CalculateBvalDigest(b3, myba_no_aggrbval_->spec_version_));
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(b3)),
            0);
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(proposal)->ExistBval(hash, 1, peers_[0]));
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(proposal)->ExistValidValue(1, hash));
    }

    void DoOnRecvPromMessageTest() {
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[3], nullptr), -1);
        PromMessagePtr msg = nullptr;

        // outdated msg
        msg = CreatePromMessage(0, peers_[3], 0, Digest(), AggregateSignature());
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -10);

        msg->seq = 1;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);

        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);

        Digest bval_digest = CalculateBvalDigest(1,
                                                 peers_[3],
                                                 0,
                                                 Digest(),
                                                 myba_no_aggrbval_->epoch_number_,
                                                 myba_no_aggrbval_->spec_version_);
        // 1.2.3 balance not enough
        Signature tmp_bval_sig = MockAggregateSign(1, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
        std::set<NodeId> tmp_ids{peers_[1], peers_[2], peers_[3]};
        std::string bitmap;
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        msg->prom_proof.bitmap = bitmap;
        msg->prom_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);
        // add new bval, now balance enough
        tmp_ids.emplace(peers_[0]);
        bitmap.clear();
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(0, bval_digest));
        msg->prom_proof.bitmap = bitmap;
        msg->prom_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            0);

        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -3);
    }

    void DoOnRecvAuxMessageTest() {
        AuxMessagePtr msg = nullptr;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -1);

        // outdated msg
        msg = CreateAuxMessage(0, peers_[3], 0, Digest(), AggregateSignature());
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -10);

        msg->seq = 1;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);

        Signature tmp_sig;
        myba_no_aggrbval_->crypto_helper_->Sign(CalculateAuxDigest(msg), tmp_sig);
        msg->signature = tmp_sig;
        Digest bval_digest = CalculateBvalDigest(1,
                                                 peers_[3],
                                                 0,
                                                 Digest(),
                                                 myba_no_aggrbval_->epoch_number_,
                                                 myba_no_aggrbval_->spec_version_);
        // 1.2.3 balance not enough
        Signature tmp_bval_sig = MockAggregateSign(1, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
        std::set<NodeId> tmp_ids{peers_[1], peers_[2], peers_[3]};
        std::string bitmap;
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        msg->aux_proof.bitmap = bitmap;
        msg->aux_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -4);
        // add new bval, now balance enough
        tmp_ids.emplace(peers_[0]);
        bitmap.clear();
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(0, bval_digest));
        msg->aux_proof.bitmap = bitmap;
        msg->aux_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            0);

        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(msg)),
            -3);
    }

    void DoFastPathTest() {
        bytes payload = asBytes("payload");
        ValMessagePtr val =
            CreateValMessage(1, peers_[1], NOWTIME, payload, myba_no_aggrbval_->epoch_number_);
        Digest hash = CalculateValMessageHash(val);
        Signature tmp_sig;
        myba_no_aggrbval_->crypto_helper_->Sign(
            CalculateBvalDigestForValMessage(val, myba_no_aggrbval_->spec_version_),
            tmp_sig);
        val->signature = tmp_sig;
        // EXPECT_TRUE(myba_no_aggrbval_->OnRecvMessage(peers_[1].first.node_id_, val) == 0);

        BvalMessagePtr bval_2 =
            CreateBvalMessage(1, peers_[1], 0, hash, false, myba_no_aggrbval_->epoch_number_);
        Digest bval_digest = CalculateBvalDigest(bval_2, myba_no_aggrbval_->spec_version_);

        // prom_1 from peers_[3]: proof with nodes 0 and 1, weight 4+3=7 (passes verification)
        // receive prom from [3], and self [0] also sends prom, still not enough (senders 0+3 weight
        // 5 < 7)
        PromMessagePtr prom_1 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        prom_1->signature = MockAggregateSign(3, CalculatePromDigest(prom_1));
        {
            Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
            MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
            std::set<NodeId> tmp_ids{peers_[0], peers_[1]};
            std::string bitmap;
            myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
            prom_1->prom_proof.bitmap = bitmap;
            prom_1->prom_proof.aggr_sig = tmp_bval_sig;
        }
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(prom_1)),
            0);

        EXPECT_TRUE(!myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());

        // prom_2 from peers_[2]: proof with nodes 0,2,3, weight 4+2+1=7 >= quorum -> decide
        PromMessagePtr prom_2 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        prom_2->signature = MockAggregateSign(2, CalculatePromDigest(prom_2));

        {
            Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
            MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
            MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
            std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2]};
            std::string bitmap;
            myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
            prom_2->prom_proof.bitmap = bitmap;
            prom_2->prom_proof.aggr_sig = tmp_bval_sig;
        }
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(prom_2)),
            0);
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(prom_2)),
            -6);

        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsLegacyCompleted());
    }

    void DoNormalPathTest1() {
        bytes payload = asBytes("payload");
        ValMessagePtr val =
            CreateValMessage(1, peers_[1], NOWTIME, payload, myba_no_aggrbval_->epoch_number_);
        Digest hash = CalculateValMessageHash(val);

        BvalMessagePtr bval_2 =
            CreateBvalMessage(1, peers_[1], 0, hash, false, myba_no_aggrbval_->epoch_number_);
        Digest bval_digest = CalculateBvalDigest(bval_2, myba_no_aggrbval_->spec_version_);
        bval_2->signature = MockAggregateSign(2, bval_digest);
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(bval_2)),
            0);

        PromMessagePtr prom_1 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        prom_1->signature = MockAggregateSign(3, CalculatePromDigest(prom_1));

        Signature tmp_bval_sig = MockAggregateSign(1, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));

        std::set<NodeId> tmp_ids{peers_[1], peers_[2], peers_[3]};
        std::string bitmap;
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        prom_1->prom_proof.bitmap = bitmap;
        prom_1->prom_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(prom_1)),
            -4);
        tmp_ids.emplace(peers_[0]);
        bitmap.clear();
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(0, bval_digest));
        prom_1->prom_proof.bitmap = bitmap;
        prom_1->prom_proof.aggr_sig = tmp_bval_sig;

        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(prom_1)),
            0);

        EXPECT_TRUE(!myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());

        AuxMessagePtr aux_3 = CreateAuxMessage(1, peers_[1], 0, hash, AggregateSignature());
        aux_3->aux_proof.bitmap = bitmap;
        aux_3->aux_proof.aggr_sig = tmp_bval_sig;
        aux_3->signature = MockAggregateSign(1, CalculateAuxDigest(aux_3));
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[1],
                                             std::make_shared<ssz_types::ConsensusMessage>(aux_3)),
            0);

        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());
        EXPECT_TRUE(!myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsLegacyCompleted());

        BvalMessagePtr bval_1_2 =
            CreateBvalMessage(1, peers_[1], 1, hash, false, myba_no_aggrbval_->epoch_number_);
        bval_1_2->signature =
            MockAggregateSign(2, CalculateBvalDigest(bval_1_2, myba_no_aggrbval_->spec_version_));
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[2],
                      std::make_shared<ssz_types::ConsensusMessage>(bval_1_2)),
                  0);
        BvalMessagePtr bval_1_1 =
            CreateBvalMessage(1, peers_[1], 1, hash, false, myba_no_aggrbval_->epoch_number_);
        bval_1_1->signature =
            MockAggregateSign(1, CalculateBvalDigest(bval_1_1, myba_no_aggrbval_->spec_version_));
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[1],
                      std::make_shared<ssz_types::ConsensusMessage>(bval_1_1)),
                  0);

        PromMessagePtr prom_1_1 = CreatePromMessage(1, peers_[1], 1, hash, AggregateSignature());
        prom_1_1->signature = MockAggregateSign(3, CalculatePromDigest(prom_1_1));

        bval_digest = CalculateBvalDigest(bval_1_1, myba_no_aggrbval_->spec_version_);
        tmp_bval_sig = MockAggregateSign(0, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
        tmp_ids = {peers_[0], peers_[1], peers_[2], peers_[3]};
        bitmap.clear();
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        prom_1_1->prom_proof.bitmap = bitmap;
        prom_1_1->prom_proof.aggr_sig = tmp_bval_sig;

        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[3],
                      std::make_shared<ssz_types::ConsensusMessage>(prom_1_1)),
                  0);

        EXPECT_TRUE(!myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsLegacyCompleted());
        PromMessagePtr prom_1_2 = CreatePromMessage(1, peers_[1], 1, hash, AggregateSignature());
        prom_1_2->signature = MockAggregateSign(2, CalculatePromDigest(prom_1_2));

        prom_1_2->prom_proof.bitmap = bitmap;
        prom_1_2->prom_proof.aggr_sig = tmp_bval_sig;

        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[2],
                      std::make_shared<ssz_types::ConsensusMessage>(prom_1_2)),
                  0);
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsLegacyCompleted());

        // bval_2 is round 0, current round is 1 after normal path -> -3 (outdated round)
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(bval_2)),
            -6);
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(aux_3)),
            -2);
    }

    void DoNormalPathTest2() {
        myba_no_aggrbval_->DoSwitchToNormalPath(0, peers_[1]);

        bytes payload = asBytes("payload");
        ValMessagePtr val =
            CreateValMessage(1, peers_[1], NOWTIME, payload, myba_no_aggrbval_->epoch_number_);
        Digest hash = CalculateValMessageHash(val);

        BvalMessagePtr bval_2 = CreateBvalMessage(1, peers_[1], 0, hash);
        bval_2->epoch_number = myba_no_aggrbval_->epoch_number_;
        Digest bval_digest = CalculateBvalDigest(bval_2, myba_no_aggrbval_->spec_version_);
        bval_2->signature = MockAggregateSign(2, bval_digest);
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(bval_2)),
            0);

        PromMessagePtr prom_1 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        prom_1->signature = MockAggregateSign(3, CalculatePromDigest(prom_1));

        Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
        std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
        std::string bitmap;
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        prom_1->prom_proof.bitmap = bitmap;
        prom_1->prom_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                             std::make_shared<ssz_types::ConsensusMessage>(prom_1)),
            0);
        // prom(hash): 1, bal=1
        // aux(hash): 1, bal=4, self
        // valid_values: {hash}

        Digest bval_zero_digest = CalculateBvalDigest(1,
                                                      peers_[1],
                                                      0,
                                                      ZERO_32_BYTES,
                                                      myba_no_aggrbval_->epoch_number_,
                                                      myba_no_aggrbval_->spec_version_);
        AuxMessagePtr aux_3 =
            CreateAuxMessage(1, peers_[1], 0, ZERO_32_BYTES, AggregateSignature());
        tmp_bval_sig = MockAggregateSign(0, bval_zero_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_zero_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_zero_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_zero_digest));
        aux_3->aux_proof.bitmap = bitmap;
        aux_3->aux_proof.aggr_sig = tmp_bval_sig;

        aux_3->signature = MockAggregateSign(2, CalculateAuxDigest(aux_3));
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                             std::make_shared<ssz_types::ConsensusMessage>(aux_3)),
            0);
        // prom(hash): 1
        // aux(hash): 1
        // aux(zero): 1
        // valid_values: {hash, zero}

        // coin: 1

        bval_2 = CreateBvalMessage(1, peers_[1], 1, hash);
        bval_2->epoch_number = myba_no_aggrbval_->epoch_number_;
        bval_digest = CalculateBvalDigest(bval_2, myba_no_aggrbval_->spec_version_);
        PromMessagePtr prom_1_1 = CreatePromMessage(1, peers_[1], 1, hash, AggregateSignature());
        prom_1_1->signature = MockAggregateSign(2, CalculatePromDigest(prom_1_1));

        tmp_bval_sig = MockAggregateSign(0, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
        prom_1_1->prom_proof.bitmap = bitmap;
        prom_1_1->prom_proof.aggr_sig = tmp_bval_sig;
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[2],
                      std::make_shared<ssz_types::ConsensusMessage>(prom_1_1)),
                  0);

        EXPECT_TRUE(!myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());

        PromMessagePtr prom_1_2 = CreatePromMessage(1, peers_[1], 1, hash, AggregateSignature());
        prom_1_2->signature = MockAggregateSign(3, CalculatePromDigest(prom_1_2));
        prom_1_2->prom_proof.bitmap = bitmap;
        prom_1_2->prom_proof.aggr_sig = tmp_bval_sig;

        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[3],
                      std::make_shared<ssz_types::ConsensusMessage>(prom_1_2)),
                  0);
        EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                      peers_[3],
                      std::make_shared<ssz_types::ConsensusMessage>(prom_1_2)),
                  -6);
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsDecided());
        EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]))->IsLegacyCompleted());
    }

    void DoEndorseTest() {
        bytes payload = asBytes("payload");
        ValMessagePtr val =
            CreateValMessage(1, peers_[1], NOWTIME, payload, myba_no_aggrbval_->epoch_number_);
        Digest hash = CalculateValMessageHash(val);

        auto proposal = ProposalKey(1, peers_[1]);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
        EXPECT_TRUE(msg_pool->ExistBval(ZERO_32_BYTES, 0, peers_[0]));
        myba_no_aggrbval_->DoEndorse(1, peers_[1], hash, true);
        EXPECT_TRUE(!msg_pool->ExistBval(hash, 0, peers_[0]));
    }

    void DoEndorseNormalTest() {
        bytes payload = asBytes("payload");
        ValMessagePtr val =
            CreateValMessage(1, peers_[1], NOWTIME, payload, myba_no_aggrbval_->epoch_number_);
        Digest hash = CalculateValMessageHash(val);

        auto proposal = ProposalKey(1, peers_[1]);
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(proposal);
        myba_no_aggrbval_->DoEndorse(1, peers_[1], hash, true);
        EXPECT_TRUE(msg_pool->ExistBval(hash, 0, peers_[0]));
    }

    void DoLoadLogTest() {
        bytes payload = asBytes("payload");
        ValMessagePtr val =
            CreateValMessage(1, peers_[1], NOWTIME, payload, myba_no_aggrbval_->epoch_number_);
        Digest hash = CalculateValMessageHash(val);
        Digest bval_digest =
            CalculateBvalDigestForValMessage(val, myba_no_aggrbval_->spec_version_);
        PromMessagePtr p1 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
        std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
        std::string bitmap;
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
        p1->prom_proof.bitmap = bitmap;
        p1->prom_proof.aggr_sig = tmp_bval_sig;
        PromMessagePtr p2 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        PromMessagePtr p0 = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
        AuxMessagePtr aux = CreateAuxMessage(1, peers_[1], 0, ZERO_32_BYTES, AggregateSignature());
        Digest bval_zero_digest = CalculateBvalDigest(1,
                                                      peers_[1],
                                                      0,
                                                      ZERO_32_BYTES,
                                                      myba_no_aggrbval_->epoch_number_,
                                                      myba_no_aggrbval_->spec_version_);
        tmp_bval_sig = MockAggregateSign(0, bval_zero_digest);
        auto m_map = deserialize_map(tmp_bval_sig);
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_zero_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_zero_digest));
        MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_zero_digest));
        aux->aux_proof.bitmap = bitmap;
        aux->aux_proof.aggr_sig = tmp_bval_sig;

        // Create AggregatedBvalMessage for testing
        ssz_types::AggregateSignature aggr_sig;
        aggr_sig.bitmap = bitmap;
        aggr_sig.aggr_sig = tmp_bval_sig;
        AggregatedBvalMessagePtr aggregated_bval =
            CreateAggregatedBvalMessage(1, peers_[1], 0, hash, std::move(aggr_sig));

        // Create AggregatedMainVoteMessage for testing with valid proof
        // Create AggrPromInfo with prom signatures (weight > 1/3)
        std::vector<ssz_types::AggrPromInfo> prom_infos;
        ssz_types::AggrPromInfo prom_info;
        prom_info.hash = hash;

        // prom_proof: the proof that nodes voted (from original prom message)
        prom_info.prom_proof.bitmap = bitmap;
        prom_info.prom_proof.aggr_sig = tmp_bval_sig;

        // aggr_sig: aggregated signatures from prom messages
        // Use nodes 0,1,2,3 which have total balance = 10 (4+3+2+1) >= quorum_balance (7)
        std::string prom_bitmap;
        std::set<NodeId> prom_nodes{peers_[0], peers_[1], peers_[2], peers_[3]};
        myba_no_aggrbval_->crypto_helper_->GenerateBitMap(prom_nodes, prom_bitmap);

        Digest prom_digest = CalculatePromDigest(p1);
        Signature prom_aggr_sig = MockAggregateSign(0, prom_digest);
        MockAggregateSignature(prom_aggr_sig, MockAggregateSign(1, prom_digest));
        MockAggregateSignature(prom_aggr_sig, MockAggregateSign(2, prom_digest));
        MockAggregateSignature(prom_aggr_sig, MockAggregateSign(3, prom_digest));

        prom_info.aggr_sig.bitmap = prom_bitmap;
        prom_info.aggr_sig.aggr_sig = prom_aggr_sig;
        prom_infos.push_back(std::move(prom_info));

        AggregatedMainVoteMessagePtr aggregated_mainvote =
            CreateAggregatedMainVoteMessage(1,
                                            peers_[1],
                                            0,
                                            std::vector<ssz_types::AggrAuxInfo>(),
                                            std::move(prom_infos));

        std::vector<std::pair<NodeId, MessagePtr>> msgs;
        msgs.push_back(
            std::make_pair(peers_[0],
                           std::make_shared<ssz_types::ConsensusMessage>(aggregated_bval)));
        msgs.push_back(
            std::make_pair(peers_[1],
                           std::make_shared<ssz_types::ConsensusMessage>(aggregated_mainvote)));
        msgs.push_back(
            std::make_pair(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(p1)));
        msgs.push_back(
            std::make_pair(peers_[2], std::make_shared<ssz_types::ConsensusMessage>(p2)));
        msgs.push_back(
            std::make_pair(peers_[3], std::make_shared<ssz_types::ConsensusMessage>(aux)));
        msgs.push_back(
            std::make_pair(peers_[0], std::make_shared<ssz_types::ConsensusMessage>(p0)));
        myba_no_aggrbval_->LoadLog(1, peers_[1], msgs);
    }

    void DoGarbageCollectionTest() {
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
        auto msg_pool_1 = std::make_shared<MyBAMessagePool>(peers_[0], 1, peers_[0], crypto_helper);
        auto msg_pool_2 = std::make_shared<MyBAMessagePool>(peers_[0], 1, peers_[1], crypto_helper);
        auto msg_pool_3 = std::make_shared<MyBAMessagePool>(peers_[0], 2, peers_[2], crypto_helper);
        myba_no_aggrbval_->ba_msg_pool_.emplace(ProposalKey(2, peers_[2]), msg_pool_3);
        myba_no_aggrbval_->ba_msg_pool_.emplace(ProposalKey(1, peers_[0]), msg_pool_1);
        myba_no_aggrbval_->ba_msg_pool_.emplace(ProposalKey(1, peers_[1]), msg_pool_2);
        EXPECT_EQ(myba_no_aggrbval_->ba_msg_pool_.size(), 3);
        myba_no_aggrbval_->DoGarbageCollection(1);
        EXPECT_EQ(myba_no_aggrbval_->ba_msg_pool_.size(), 1);
        EXPECT_TRUE(myba_no_aggrbval_->ba_msg_pool_.count(ProposalKey(2, peers_[2])));
    }

    void DoDecideTest() {
        auto msg_pool = myba_no_aggrbval_->GetMsgPool(ProposalKey(0, peers_[1]));
        myba_no_aggrbval_->Decide(false, 0, msg_pool, "test_hash", nullptr);
        EXPECT_TRUE(msg_pool->decide_value_ == "test_hash");
        EXPECT_TRUE(!msg_pool->IsLegacyCompleted());
        myba_no_aggrbval_->Decide(false, 0, msg_pool, "test_hash", nullptr);
        EXPECT_TRUE(msg_pool->IsLegacyCompleted());
        myba_no_aggrbval_->Decide(true, 0, msg_pool, "another_hash", nullptr);
        EXPECT_TRUE(msg_pool->decide_value_ == "test_hash");
    }
};

TEST_F(MyBATest, OnRecvBvalMessageTest) {
    DoOnRecvBvalMessageTest();
}

TEST_F(MyBATest, OnRecvPromMessageTest) {
    DoOnRecvPromMessageTest();
}

TEST_F(MyBATest, OnRecvAuxMessageTest) {
    DoOnRecvAuxMessageTest();
}

TEST_F(MyBATest, FastPathTest) {
    DoFastPathTest();
}

TEST_F(MyBATest, NormalPathTest1) {
    DoNormalPathTest1();
}

TEST_F(MyBATest, NormalPathTest2) {
    DoNormalPathTest2();
}

TEST_F(MyBATest, NormalPathTest3) {
    // my prom + prom + aux
    for (auto& peer : *peer_balance_map_) {
        peer.second = 1;
    }
    std::shared_ptr<MockReliableChannel> mock_channel = std::make_shared<MockReliableChannel>();
    myba_no_aggrbval_ = std::make_shared<MyBA>(
        10,
        6,  // epoch
        my_id_,
        4,
        1,  // tolerable_faulty_balance
        3,  // quorum_balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        mock_channel);

    bytes payload = asBytes("payload");
    NodeId proposer = peers_[0];
    ValMessagePtr val =
        CreateValMessage(1, proposer, NOWTIME, payload, myba_no_aggrbval_->spec_version_);
    Digest hash = CalculateValMessageHash(val);
    Digest bval_digest;
    // 3 bvals
    for (int i = 0; i < 3; ++i) {
        BvalMessagePtr bval_0 =
            CreateBvalMessage(1, proposer, 0, hash, true, myba_no_aggrbval_->spec_version_);
        bval_digest = CalculateBvalDigest(bval_0, myba_no_aggrbval_->spec_version_);
        bval_0->signature = MockAggregateSign(i, bval_digest);
        EXPECT_EQ(
            myba_no_aggrbval_->OnRecvMessage(peers_[i],
                                             std::make_shared<ssz_types::ConsensusMessage>(bval_0)),
            0);
    }

    // one prom
    PromMessagePtr prom_1 = CreatePromMessage(1, proposer, 0, hash, AggregateSignature());
    prom_1->signature = MockAggregateSign(1, CalculatePromDigest(prom_1));

    Signature tmp_bval_sig = MockAggregateSign(1, bval_digest);
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));

    std::set<NodeId> tmp_ids{peers_[1], peers_[2], peers_[3]};
    std::string bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);
    prom_1->prom_proof.bitmap = bitmap;
    prom_1->prom_proof.aggr_sig = tmp_bval_sig;
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[1],
                                         std::make_shared<ssz_types::ConsensusMessage>(prom_1)),
        0);

    // one aux
    AuxMessagePtr aux_2 = CreateAuxMessage(1, proposer, 0, hash, AggregateSignature());
    aux_2->aux_proof.bitmap = bitmap;
    aux_2->aux_proof.aggr_sig = tmp_bval_sig;
    aux_2->signature = MockAggregateSign(2, CalculateAuxDigest(aux_2));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                         std::make_shared<ssz_types::ConsensusMessage>(aux_2)),
        0);
    EXPECT_TRUE(myba_no_aggrbval_->GetMsgPool(ProposalKey(1, proposer))->IsDecided());
    auto last_msg = mock_channel->last_broadcast_msg_;
    EXPECT_EQ(last_msg->Index(), ConsensusMessageType::AggregatedMainVoteMessage);
    EXPECT_EQ(last_msg->AggregatedMainVoteData()->seq, 1);
    EXPECT_EQ(last_msg->AggregatedMainVoteData()->proposer_id.Acquire(), proposer);
    EXPECT_EQ(last_msg->AggregatedMainVoteData()->round, 0);
}

TEST_F(MyBATest, EndorseTest) {
    DoEndorseTest();
}

TEST_F(MyBATest, EndorseNormalTest) {
    DoEndorseNormalTest();
}

TEST_F(MyBATest, LoadLogTest) {
    DoLoadLogTest();
}

TEST_F(MyBATest, GarbageCollectionTest) {
    DoGarbageCollectionTest();
}

TEST_F(MyBATest, DecideTest) {
    DoDecideTest();
}

TEST_F(MyBATest, OnRecvAggregatedBvalMessageTest) {
    // Test 1: nullptr message
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[1], nullptr), -1);

    // Test 2: invalid aggregated_bval_msg (nullptr)
    MessagePtr consensus_msg = std::make_shared<ssz_types::ConsensusMessage>();
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[1], consensus_msg), -99);

    // Prepare valid aggregated bval message (spec_version 7 digest)
    Digest hash = Digest("test_hash");
    Digest bval_digest = CalculateBvalDigest(1,
                                             peers_[1],
                                             0,
                                             hash,
                                             myba_no_aggrbval_->epoch_number_,
                                             myba_no_aggrbval_->spec_version_);

    // Create aggregated signature with quorum balance (nodes 0,1,2,3 = 10 total)
    Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
    std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
    std::string bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);

    // Test 3: outdated message (seq <= stable_seq)
    ssz_types::AggregateSignature outdated_aggr_sig;
    outdated_aggr_sig.bitmap = bitmap;
    outdated_aggr_sig.aggr_sig = tmp_bval_sig;
    AggregatedBvalMessagePtr outdated_msg =
        CreateAggregatedBvalMessage(0, peers_[1], 0, hash, std::move(outdated_aggr_sig));
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(outdated_msg)),
              -10);

    // Test 4: CheckCompleted returns true (already completed)
    ssz_types::AggregateSignature test4_aggr_sig;
    test4_aggr_sig.bitmap = bitmap;
    test4_aggr_sig.aggr_sig = tmp_bval_sig;
    AggregatedBvalMessagePtr msg =
        CreateAggregatedBvalMessage(1, peers_[1], 0, hash, std::move(test4_aggr_sig));
    auto msg_pool = myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]));

    // Make it completed
    msg_pool->legacy_completed_ = true;
    auto aggregated_mainvote =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::vector<ssz_types::AggrPromInfo>());
    msg_pool->aggregated_mainvote_msg_[0] = aggregated_mainvote;
    msg_pool->decision_certificate_ = aggregated_mainvote;

    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                               std::make_shared<ssz_types::ConsensusMessage>(msg)),
              -6);

    // Reset for next tests
    msg_pool->legacy_completed_ = false;
    msg_pool->aggregated_mainvote_msg_.clear();
    msg_pool->decision_certificate_ = nullptr;

    // Test 5: LegacyVersionBeforeDecisionProof and IsLegacyCompleted
    msg_pool->legacy_completed_ = true;
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                               std::make_shared<ssz_types::ConsensusMessage>(msg)),
              -3);
    msg_pool->legacy_completed_ = false;

    // Test 6: ExistAggregatedBval (already received)
    msg_pool->aggregated_bval_msg_[0] = msg;
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                               std::make_shared<ssz_types::ConsensusMessage>(msg)),
              -3);
    msg_pool->aggregated_bval_msg_.clear();

    // Test 7: Invalid proof - CheckQuorumBalance fails (insufficient balance)
    Signature insufficient_bval_sig = MockAggregateSign(1, bval_digest);
    MockAggregateSignature(insufficient_bval_sig, MockAggregateSign(2, bval_digest));
    std::set<NodeId> insufficient_ids{peers_[1], peers_[2]};  // balance = 3+2 = 5 < 7
    std::string insufficient_bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(insufficient_ids, insufficient_bitmap);
    ssz_types::AggregateSignature insufficient_sig;
    insufficient_sig.bitmap = insufficient_bitmap;
    insufficient_sig.aggr_sig = insufficient_bval_sig;

    AggregatedBvalMessagePtr insufficient_msg =
        CreateAggregatedBvalMessage(1, peers_[1], 0, hash, std::move(insufficient_sig));
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(insufficient_msg)),
              -4);

    // Test 8: Invalid proof - VerifyPhaseOneQC fails (wrong signature)
    Digest wrong_digest = Digest("wrong_digest");
    Signature wrong_bval_sig = MockAggregateSign(0, wrong_digest);
    MockAggregateSignature(wrong_bval_sig, MockAggregateSign(1, wrong_digest));
    MockAggregateSignature(wrong_bval_sig, MockAggregateSign(2, wrong_digest));
    MockAggregateSignature(wrong_bval_sig, MockAggregateSign(3, wrong_digest));
    ssz_types::AggregateSignature wrong_sig;
    wrong_sig.bitmap = bitmap;
    wrong_sig.aggr_sig = wrong_bval_sig;

    AggregatedBvalMessagePtr wrong_sig_msg =
        CreateAggregatedBvalMessage(1, peers_[1], 0, hash, std::move(wrong_sig));
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(wrong_sig_msg)),
              -4);

    // Test 9: Successful reception
    ssz_types::AggregateSignature test9_aggr_sig;
    test9_aggr_sig.bitmap = bitmap;
    test9_aggr_sig.aggr_sig = tmp_bval_sig;
    AggregatedBvalMessagePtr valid_msg =
        CreateAggregatedBvalMessage(1, peers_[1], 0, hash, std::move(test9_aggr_sig));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                         std::make_shared<ssz_types::ConsensusMessage>(valid_msg)),
        0);

    // Verify the message was added to msg_pool
    EXPECT_TRUE(msg_pool->ExistAggregatedBval(0));
    EXPECT_TRUE(msg_pool->ExistValidValue(0, hash));

    // Test 10: Successful reception from my_id (should skip proof verification)
    auto msg_pool_2 = myba_no_aggrbval_->GetMsgPool(ProposalKey(2, peers_[1]));
    ssz_types::AggregateSignature test10_aggr_sig;
    test10_aggr_sig.bitmap = bitmap;
    test10_aggr_sig.aggr_sig = tmp_bval_sig;
    AggregatedBvalMessagePtr my_msg =
        CreateAggregatedBvalMessage(2, peers_[1], 0, hash, std::move(test10_aggr_sig));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[0],  // my_id
                                         std::make_shared<ssz_types::ConsensusMessage>(my_msg)),
        0);
    EXPECT_TRUE(msg_pool_2->ExistAggregatedBval(0));

    // Test 11: Different seq (3)
    auto msg_pool_no_aggrbval = myba_no_aggrbval_->GetMsgPool(ProposalKey(3, peers_[1]));
    Digest bval_digest_no_aggrbval = CalculateBvalDigest(3,
                                                         peers_[1],
                                                         0,
                                                         hash,
                                                         myba_no_aggrbval_->epoch_number_,
                                                         myba_no_aggrbval_->spec_version_);

    Signature tmp_bval_sig_no_aggrbval = MockAggregateSign(0, bval_digest_no_aggrbval);
    MockAggregateSignature(tmp_bval_sig_no_aggrbval, MockAggregateSign(1, bval_digest_no_aggrbval));
    MockAggregateSignature(tmp_bval_sig_no_aggrbval, MockAggregateSign(2, bval_digest_no_aggrbval));
    MockAggregateSignature(tmp_bval_sig_no_aggrbval, MockAggregateSign(3, bval_digest_no_aggrbval));

    ssz_types::AggregateSignature aggr_sig_no_aggrbval;
    aggr_sig_no_aggrbval.bitmap = bitmap;
    aggr_sig_no_aggrbval.aggr_sig = tmp_bval_sig_no_aggrbval;

    AggregatedBvalMessagePtr msg_no_aggrbval =
        CreateAggregatedBvalMessage(3, peers_[1], 0, hash, std::move(aggr_sig_no_aggrbval));
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(msg_no_aggrbval)),
              0);
    EXPECT_TRUE(msg_pool_no_aggrbval->ExistAggregatedBval(0));
}

TEST_F(MyBATest, VerifyAggregatedMainVoteTest) {
    Digest hash = Digest("test_hash");

    // Prepare common signatures (spec_version 7)
    Digest bval_digest = CalculateBvalDigest(1,
                                             peers_[1],
                                             0,
                                             hash,
                                             myba_no_aggrbval_->epoch_number_,
                                             myba_no_aggrbval_->spec_version_);
    Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));

    // Use nodes 0,1,2,3 which have total balance = 10 (4+3+2+1) >= quorum_balance (7)
    std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
    std::string bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);

    PromMessagePtr tmp_prom = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
    Digest prom_digest = CalculatePromDigest(tmp_prom);
    Signature prom_aggr_sig = MockAggregateSign(0, prom_digest);
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(1, prom_digest));
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(2, prom_digest));
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(3, prom_digest));

    AuxMessagePtr tmp_aux = CreateAuxMessage(1, peers_[1], 0, hash, AggregateSignature());
    Digest aux_digest = CalculateAuxDigest(tmp_aux);
    Signature aux_aggr_sig = MockAggregateSign(0, aux_digest);
    MockAggregateSignature(aux_aggr_sig, MockAggregateSign(1, aux_digest));
    MockAggregateSignature(aux_aggr_sig, MockAggregateSign(2, aux_digest));
    MockAggregateSignature(aux_aggr_sig, MockAggregateSign(3, aux_digest));

    // Test 1: Empty message (no prom and no aux) - should fail due to insufficient balance
    AggregatedMainVoteMessagePtr empty_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::vector<ssz_types::AggrPromInfo>());
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(empty_msg));

    // Test 2: Valid message with only prom info
    std::vector<ssz_types::AggrPromInfo> prom_infos_only;
    ssz_types::AggrPromInfo prom_info_only;
    prom_info_only.hash = hash;
    prom_info_only.prom_proof.bitmap = bitmap;
    prom_info_only.prom_proof.aggr_sig = tmp_bval_sig;
    prom_info_only.aggr_sig.bitmap = bitmap;
    prom_info_only.aggr_sig.aggr_sig = prom_aggr_sig;
    prom_infos_only.push_back(std::move(prom_info_only));

    AggregatedMainVoteMessagePtr prom_only_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_only));
    EXPECT_TRUE(myba_no_aggrbval_->VerifyAggregatedMainVote(prom_only_msg));

    // Test 3: Valid message with only aux info
    std::vector<ssz_types::AggrAuxInfo> aux_infos_only;
    ssz_types::AggrAuxInfo aux_info_only;
    aux_info_only.hash = hash;
    aux_info_only.aux_proof.bitmap = bitmap;
    aux_info_only.aux_proof.aggr_sig = tmp_bval_sig;
    aux_info_only.aggr_sig.bitmap = bitmap;
    aux_info_only.aggr_sig.aggr_sig = aux_aggr_sig;
    aux_infos_only.push_back(std::move(aux_info_only));

    AggregatedMainVoteMessagePtr aux_only_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::move(aux_infos_only),
                                        std::vector<ssz_types::AggrPromInfo>());
    EXPECT_TRUE(myba_no_aggrbval_->VerifyAggregatedMainVote(aux_only_msg));

    // Test 4: Valid message with both prom and aux info (different nodes)
    // Use nodes 0,1 for prom (balance = 4+3 = 7) and nodes 2,3 for aux (balance = 2+1 = 3)
    // Total balance = 10 >= quorum_balance (7)
    std::set<NodeId> prom_ids{peers_[0], peers_[1]};
    std::string prom_bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(prom_ids, prom_bitmap);
    Signature prom_sig_01 = MockAggregateSign(0, prom_digest);
    MockAggregateSignature(prom_sig_01, MockAggregateSign(1, prom_digest));

    std::set<NodeId> aux_ids{peers_[2], peers_[3]};
    std::string aux_bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(aux_ids, aux_bitmap);
    Signature aux_sig_23 = MockAggregateSign(2, aux_digest);
    MockAggregateSignature(aux_sig_23, MockAggregateSign(3, aux_digest));

    Signature prom_bval_sig_01 = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(prom_bval_sig_01, MockAggregateSign(1, bval_digest));

    Signature aux_bval_sig_23 = MockAggregateSign(2, bval_digest);
    MockAggregateSignature(aux_bval_sig_23, MockAggregateSign(3, bval_digest));

    std::vector<ssz_types::AggrPromInfo> prom_infos_mixed;
    ssz_types::AggrPromInfo prom_info_mixed;
    prom_info_mixed.hash = hash;
    prom_info_mixed.prom_proof.bitmap = prom_bitmap;
    prom_info_mixed.prom_proof.aggr_sig = prom_bval_sig_01;
    prom_info_mixed.aggr_sig.bitmap = prom_bitmap;
    prom_info_mixed.aggr_sig.aggr_sig = prom_sig_01;
    prom_infos_mixed.push_back(std::move(prom_info_mixed));

    std::vector<ssz_types::AggrAuxInfo> aux_infos_mixed;
    ssz_types::AggrAuxInfo aux_info_mixed;
    aux_info_mixed.hash = hash;
    aux_info_mixed.aux_proof.bitmap = aux_bitmap;
    aux_info_mixed.aux_proof.aggr_sig = aux_bval_sig_23;
    aux_info_mixed.aggr_sig.bitmap = aux_bitmap;
    aux_info_mixed.aggr_sig.aggr_sig = aux_sig_23;
    aux_infos_mixed.push_back(std::move(aux_info_mixed));

    AggregatedMainVoteMessagePtr mixed_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::move(aux_infos_mixed),
                                        std::move(prom_infos_mixed));
    EXPECT_TRUE(myba_no_aggrbval_->VerifyAggregatedMainVote(mixed_msg));

    // Test 5: Invalid prom signature
    std::vector<ssz_types::AggrPromInfo> invalid_prom_infos;
    ssz_types::AggrPromInfo invalid_prom_info;
    invalid_prom_info.hash = hash;
    invalid_prom_info.prom_proof.bitmap = bitmap;
    invalid_prom_info.prom_proof.aggr_sig = tmp_bval_sig;
    invalid_prom_info.aggr_sig.bitmap = bitmap;
    // Use wrong signature
    Digest wrong_digest = Digest("wrong_hash");
    Signature wrong_prom_sig = MockAggregateSign(0, wrong_digest);
    MockAggregateSignature(wrong_prom_sig, MockAggregateSign(1, wrong_digest));
    invalid_prom_info.aggr_sig.aggr_sig = wrong_prom_sig;
    invalid_prom_infos.push_back(std::move(invalid_prom_info));

    AggregatedMainVoteMessagePtr invalid_prom_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(invalid_prom_infos));
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(invalid_prom_msg));

    // Test 6: Invalid aux signature
    std::vector<ssz_types::AggrAuxInfo> invalid_aux_infos;
    ssz_types::AggrAuxInfo invalid_aux_info;
    invalid_aux_info.hash = hash;
    invalid_aux_info.aux_proof.bitmap = bitmap;
    invalid_aux_info.aux_proof.aggr_sig = tmp_bval_sig;
    invalid_aux_info.aggr_sig.bitmap = bitmap;
    // Use wrong signature
    Signature wrong_aux_sig = MockAggregateSign(0, wrong_digest);
    MockAggregateSignature(wrong_aux_sig, MockAggregateSign(1, wrong_digest));
    invalid_aux_info.aggr_sig.aggr_sig = wrong_aux_sig;
    invalid_aux_infos.push_back(std::move(invalid_aux_info));

    AggregatedMainVoteMessagePtr invalid_aux_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::move(invalid_aux_infos),
                                        std::vector<ssz_types::AggrPromInfo>());
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(invalid_aux_msg));

    // Test 7: Duplicated nodes (same node in both prom and aux)
    std::vector<ssz_types::AggrPromInfo> dup_prom_infos;
    ssz_types::AggrPromInfo dup_prom_info;
    dup_prom_info.hash = hash;
    dup_prom_info.prom_proof.bitmap = bitmap;
    dup_prom_info.prom_proof.aggr_sig = tmp_bval_sig;
    dup_prom_info.aggr_sig.bitmap = bitmap;
    dup_prom_info.aggr_sig.aggr_sig = prom_aggr_sig;
    dup_prom_infos.push_back(std::move(dup_prom_info));

    std::vector<ssz_types::AggrAuxInfo> dup_aux_infos;
    ssz_types::AggrAuxInfo dup_aux_info;
    dup_aux_info.hash = hash;
    dup_aux_info.aux_proof.bitmap = bitmap;  // Same bitmap as prom
    dup_aux_info.aux_proof.aggr_sig = tmp_bval_sig;
    dup_aux_info.aggr_sig.bitmap = bitmap;
    dup_aux_info.aggr_sig.aggr_sig = aux_aggr_sig;
    dup_aux_infos.push_back(std::move(dup_aux_info));

    AggregatedMainVoteMessagePtr dup_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::move(dup_aux_infos),
                                        std::move(dup_prom_infos));
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(dup_msg));

    // Test 8: Insufficient balance (only nodes 1,2,3, balance = 3+2+1 = 6 < 7)
    std::set<NodeId> insufficient_ids{peers_[1], peers_[2], peers_[3]};
    std::string insufficient_bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(insufficient_ids, insufficient_bitmap);

    Signature insufficient_prom_sig = MockAggregateSign(1, prom_digest);
    MockAggregateSignature(insufficient_prom_sig, MockAggregateSign(2, prom_digest));
    MockAggregateSignature(insufficient_prom_sig, MockAggregateSign(3, prom_digest));

    Signature insufficient_bval_sig = MockAggregateSign(1, bval_digest);
    MockAggregateSignature(insufficient_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(insufficient_bval_sig, MockAggregateSign(3, bval_digest));

    std::vector<ssz_types::AggrPromInfo> insufficient_prom_infos;
    ssz_types::AggrPromInfo insufficient_prom_info;
    insufficient_prom_info.hash = hash;
    insufficient_prom_info.prom_proof.bitmap = insufficient_bitmap;
    insufficient_prom_info.prom_proof.aggr_sig = insufficient_bval_sig;
    insufficient_prom_info.aggr_sig.bitmap = insufficient_bitmap;
    insufficient_prom_info.aggr_sig.aggr_sig = insufficient_prom_sig;
    insufficient_prom_infos.push_back(std::move(insufficient_prom_info));

    AggregatedMainVoteMessagePtr insufficient_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(insufficient_prom_infos));
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(insufficient_msg));

    // Test 9: Multiple prom infos with different hashes — sub-quorum inner proof rejected
    // hash uses peers_[0]+peers_[1] (balance=7, quorum), hash2 uses peers_[2]+peers_[3] (balance=3,
    // sub-quorum). With CheckQuorumBalance on inner proofs, hash2's sub-quorum bval QC is rejected.
    Digest hash2 = Digest("test_hash_2");
    PromMessagePtr tmp_prom2 = CreatePromMessage(1, peers_[1], 0, hash2, AggregateSignature());
    Digest prom_digest2 = CalculatePromDigest(tmp_prom2);

    std::set<NodeId> prom_ids1{peers_[0], peers_[1]};
    std::string prom_bitmap1;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(prom_ids1, prom_bitmap1);
    Signature prom_sig1 = MockAggregateSign(0, prom_digest);
    MockAggregateSignature(prom_sig1, MockAggregateSign(1, prom_digest));
    Signature prom_bval_sig1 = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(prom_bval_sig1, MockAggregateSign(1, bval_digest));

    std::set<NodeId> prom_ids2{peers_[2], peers_[3]};
    std::string prom_bitmap2;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(prom_ids2, prom_bitmap2);
    Digest bval_digest2 = CalculateBvalDigest(1,
                                              peers_[1],
                                              0,
                                              hash2,
                                              myba_no_aggrbval_->epoch_number_,
                                              myba_no_aggrbval_->spec_version_);
    Signature prom_sig2 = MockAggregateSign(2, prom_digest2);
    MockAggregateSignature(prom_sig2, MockAggregateSign(3, prom_digest2));
    Signature prom_bval_sig2 = MockAggregateSign(2, bval_digest2);
    MockAggregateSignature(prom_bval_sig2, MockAggregateSign(3, bval_digest2));

    std::vector<ssz_types::AggrPromInfo> multi_prom_infos;
    ssz_types::AggrPromInfo prom_info1;
    prom_info1.hash = hash;
    prom_info1.prom_proof.bitmap = prom_bitmap1;
    prom_info1.prom_proof.aggr_sig = prom_bval_sig1;
    prom_info1.aggr_sig.bitmap = prom_bitmap1;
    prom_info1.aggr_sig.aggr_sig = prom_sig1;
    multi_prom_infos.push_back(std::move(prom_info1));

    ssz_types::AggrPromInfo prom_info2;
    prom_info2.hash = hash2;
    prom_info2.prom_proof.bitmap = prom_bitmap2;
    prom_info2.prom_proof.aggr_sig = prom_bval_sig2;
    prom_info2.aggr_sig.bitmap = prom_bitmap2;
    prom_info2.aggr_sig.aggr_sig = prom_sig2;
    multi_prom_infos.push_back(std::move(prom_info2));

    AggregatedMainVoteMessagePtr multi_prom_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(multi_prom_infos));
    // hash2's inner bval QC has only peers_[2]+peers_[3] (balance=3 < quorum=7), rejected
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(multi_prom_msg));

    // Test 10: Multiple aux infos with different hashes — sub-quorum inner proof rejected
    AuxMessagePtr tmp_aux2 = CreateAuxMessage(1, peers_[1], 0, hash2, AggregateSignature());
    Digest aux_digest2 = CalculateAuxDigest(tmp_aux2);

    std::set<NodeId> aux_ids1{peers_[0], peers_[1]};
    std::string aux_bitmap1;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(aux_ids1, aux_bitmap1);
    Signature aux_sig1 = MockAggregateSign(0, aux_digest);
    MockAggregateSignature(aux_sig1, MockAggregateSign(1, aux_digest));
    Signature aux_bval_sig1 = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(aux_bval_sig1, MockAggregateSign(1, bval_digest));

    std::set<NodeId> aux_ids2{peers_[2], peers_[3]};
    std::string aux_bitmap2;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(aux_ids2, aux_bitmap2);
    Signature aux_sig2 = MockAggregateSign(2, aux_digest2);
    MockAggregateSignature(aux_sig2, MockAggregateSign(3, aux_digest2));
    Signature aux_bval_sig2 = MockAggregateSign(2, bval_digest2);
    MockAggregateSignature(aux_bval_sig2, MockAggregateSign(3, bval_digest2));

    std::vector<ssz_types::AggrAuxInfo> multi_aux_infos;
    ssz_types::AggrAuxInfo aux_info1;
    aux_info1.hash = hash;
    aux_info1.aux_proof.bitmap = aux_bitmap1;
    aux_info1.aux_proof.aggr_sig = aux_bval_sig1;
    aux_info1.aggr_sig.bitmap = aux_bitmap1;
    aux_info1.aggr_sig.aggr_sig = aux_sig1;
    multi_aux_infos.push_back(std::move(aux_info1));

    ssz_types::AggrAuxInfo aux_info2;
    aux_info2.hash = hash2;
    aux_info2.aux_proof.bitmap = aux_bitmap2;
    aux_info2.aux_proof.aggr_sig = aux_bval_sig2;
    aux_info2.aggr_sig.bitmap = aux_bitmap2;
    aux_info2.aggr_sig.aggr_sig = aux_sig2;
    multi_aux_infos.push_back(std::move(aux_info2));

    AggregatedMainVoteMessagePtr multi_aux_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::move(multi_aux_infos),
                                        std::vector<ssz_types::AggrPromInfo>());
    // hash2's inner bval QC has only peers_[2]+peers_[3] (balance=3 < quorum=7), rejected
    EXPECT_FALSE(myba_no_aggrbval_->VerifyAggregatedMainVote(multi_aux_msg));
}

TEST_F(MyBATest, OnRecvAggregatedMainVoteMessageTest) {
    // Test 1: nullptr message
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[1], nullptr), -1);

    // Test 2: invalid aggregated_mainvote_msg (nullptr)
    MessagePtr consensus_msg = std::make_shared<ssz_types::ConsensusMessage>();
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[1], consensus_msg), -99);

    // Prepare valid aggregated mainvote message (spec_version 7)
    Digest hash = Digest("test_hash");

    // Create prom info with valid proof
    Digest bval_digest = CalculateBvalDigest(1,
                                             peers_[1],
                                             0,
                                             hash,
                                             myba_no_aggrbval_->epoch_number_,
                                             myba_no_aggrbval_->spec_version_);
    Signature tmp_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(tmp_bval_sig, MockAggregateSign(3, bval_digest));
    std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
    std::string bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);

    // Create prom signatures
    PromMessagePtr tmp_prom = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
    Digest prom_digest = CalculatePromDigest(tmp_prom);
    Signature prom_aggr_sig = MockAggregateSign(0, prom_digest);
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(1, prom_digest));
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(2, prom_digest));
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(3, prom_digest));

    // Test 3: outdated message (seq <= stable_seq)
    AggregatedMainVoteMessagePtr outdated_msg =
        CreateAggregatedMainVoteMessage(0,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::vector<ssz_types::AggrPromInfo>());
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(outdated_msg)),
              -10);

    // Test 4: LegacyVersionBeforeDecisionProof and IsLegacyCompleted
    std::vector<ssz_types::AggrPromInfo> prom_infos_copy1;
    ssz_types::AggrPromInfo prom_info_copy1;
    prom_info_copy1.hash = hash;
    prom_info_copy1.prom_proof.bitmap = bitmap;
    prom_info_copy1.prom_proof.aggr_sig = tmp_bval_sig;
    prom_info_copy1.aggr_sig.bitmap = bitmap;
    prom_info_copy1.aggr_sig.aggr_sig = prom_aggr_sig;
    prom_infos_copy1.push_back(std::move(prom_info_copy1));

    AggregatedMainVoteMessagePtr msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_copy1));
    auto msg_pool = myba_no_aggrbval_->GetMsgPool(ProposalKey(1, peers_[1]));
    msg_pool->legacy_completed_ = true;

    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                               std::make_shared<ssz_types::ConsensusMessage>(msg)),
              -3);
    msg_pool->legacy_completed_ = false;

    // Test 5: LegacyVersionBeforeDecisionProof and ExistAggregatedMainVote with aux
    std::vector<ssz_types::AggrAuxInfo> aux_infos;
    ssz_types::AggrAuxInfo aux_info;
    aux_info.hash = hash;
    aux_info.aux_proof.bitmap = bitmap;
    aux_info.aux_proof.aggr_sig = tmp_bval_sig;
    aux_info.aggr_sig.bitmap = bitmap;
    AuxMessagePtr tmp_aux = CreateAuxMessage(1, peers_[1], 0, hash, AggregateSignature());
    Digest aux_digest = CalculateAuxDigest(tmp_aux);
    Signature aux_aggr_sig = MockAggregateSign(0, aux_digest);
    MockAggregateSignature(aux_aggr_sig, MockAggregateSign(1, aux_digest));
    MockAggregateSignature(aux_aggr_sig, MockAggregateSign(2, aux_digest));
    MockAggregateSignature(aux_aggr_sig, MockAggregateSign(3, aux_digest));
    aux_info.aggr_sig.aggr_sig = aux_aggr_sig;
    aux_infos.push_back(std::move(aux_info));

    AggregatedMainVoteMessagePtr existing_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::vector<ssz_types::AggrPromInfo>());
    msg_pool->aggregated_mainvote_msg_[0] = existing_msg;

    AggregatedMainVoteMessagePtr msg_with_aux =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::move(aux_infos),
                                        std::vector<ssz_types::AggrPromInfo>());
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(msg_with_aux)),
              -3);
    msg_pool->aggregated_mainvote_msg_.clear();

    // Test 6: Non-legacy version and IsDecided
    // First, let myba_no_aggrbval_ decide on seq=2
    auto msg_pool_no_aggrbval = myba_no_aggrbval_->GetMsgPool(ProposalKey(2, peers_[1]));

    // Create bval signatures for myba_no_aggrbval_ (spec_version 7)
    Digest bval_digest_no_aggrbval = CalculateBvalDigest(2,
                                                         peers_[1],
                                                         0,
                                                         hash,
                                                         myba_no_aggrbval_->epoch_number_,
                                                         myba_no_aggrbval_->spec_version_);
    Signature tmp_bval_sig_no_aggrbval = MockAggregateSign(0, bval_digest_no_aggrbval);
    MockAggregateSignature(tmp_bval_sig_no_aggrbval, MockAggregateSign(1, bval_digest_no_aggrbval));
    MockAggregateSignature(tmp_bval_sig_no_aggrbval, MockAggregateSign(2, bval_digest_no_aggrbval));
    MockAggregateSignature(tmp_bval_sig_no_aggrbval, MockAggregateSign(3, bval_digest_no_aggrbval));

    // Create prom signatures for myba_no_aggrbval_
    PromMessagePtr tmp_prom_no_aggrbval =
        CreatePromMessage(2, peers_[1], 0, hash, AggregateSignature());
    Digest prom_digest_no_aggrbval = CalculatePromDigest(tmp_prom_no_aggrbval);
    Signature prom_aggr_sig_no_aggrbval = MockAggregateSign(0, prom_digest_no_aggrbval);
    MockAggregateSignature(prom_aggr_sig_no_aggrbval,
                           MockAggregateSign(1, prom_digest_no_aggrbval));
    MockAggregateSignature(prom_aggr_sig_no_aggrbval,
                           MockAggregateSign(2, prom_digest_no_aggrbval));
    MockAggregateSignature(prom_aggr_sig_no_aggrbval,
                           MockAggregateSign(3, prom_digest_no_aggrbval));

    std::vector<ssz_types::AggrPromInfo> prom_infos_first;
    ssz_types::AggrPromInfo prom_info_first;
    prom_info_first.hash = hash;
    prom_info_first.prom_proof.bitmap = bitmap;
    prom_info_first.prom_proof.aggr_sig = tmp_bval_sig_no_aggrbval;
    prom_info_first.aggr_sig.bitmap = bitmap;
    prom_info_first.aggr_sig.aggr_sig = prom_aggr_sig_no_aggrbval;
    prom_infos_first.push_back(std::move(prom_info_first));

    // Send first message to trigger decide
    AggregatedMainVoteMessagePtr first_msg =
        CreateAggregatedMainVoteMessage(2,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_first));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                         std::make_shared<ssz_types::ConsensusMessage>(first_msg)),
        0);
    EXPECT_TRUE(msg_pool_no_aggrbval->IsDecided());

    // Now send another message, should return -3 because already decided
    std::vector<ssz_types::AggrPromInfo> prom_infos_second;
    ssz_types::AggrPromInfo prom_info_second;
    prom_info_second.hash = hash;
    prom_info_second.prom_proof.bitmap = bitmap;
    prom_info_second.prom_proof.aggr_sig = tmp_bval_sig_no_aggrbval;
    prom_info_second.aggr_sig.bitmap = bitmap;
    prom_info_second.aggr_sig.aggr_sig = prom_aggr_sig_no_aggrbval;
    prom_infos_second.push_back(std::move(prom_info_second));

    AggregatedMainVoteMessagePtr second_msg =
        CreateAggregatedMainVoteMessage(2,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_second));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[3],
                                         std::make_shared<ssz_types::ConsensusMessage>(second_msg)),
        -3);

    // Test 7: Invalid proof - VerifyAggregatedMainVote fails
    std::vector<ssz_types::AggrPromInfo> invalid_prom_infos;
    ssz_types::AggrPromInfo invalid_prom_info;
    invalid_prom_info.hash = hash;
    // Use insufficient balance (only 2 nodes, balance = 3+2 = 5 < 7)
    Signature insufficient_bval_sig = MockAggregateSign(1, bval_digest);
    MockAggregateSignature(insufficient_bval_sig, MockAggregateSign(2, bval_digest));
    std::set<NodeId> insufficient_ids{peers_[1], peers_[2]};
    std::string insufficient_bitmap;
    myba_no_aggrbval_->crypto_helper_->GenerateBitMap(insufficient_ids, insufficient_bitmap);
    invalid_prom_info.prom_proof.bitmap = insufficient_bitmap;
    invalid_prom_info.prom_proof.aggr_sig = insufficient_bval_sig;
    invalid_prom_info.aggr_sig.bitmap = insufficient_bitmap;
    Signature insufficient_prom_sig = MockAggregateSign(1, prom_digest);
    MockAggregateSignature(insufficient_prom_sig, MockAggregateSign(2, prom_digest));
    invalid_prom_info.aggr_sig.aggr_sig = insufficient_prom_sig;
    invalid_prom_infos.push_back(std::move(invalid_prom_info));

    AggregatedMainVoteMessagePtr invalid_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(invalid_prom_infos));
    EXPECT_EQ(myba_no_aggrbval_->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(invalid_msg)),
              -4);

    // Test 8: Successful reception - pure prom (fast path decision)
    std::vector<ssz_types::AggrPromInfo> prom_infos_copy3;
    ssz_types::AggrPromInfo prom_info_copy3;
    prom_info_copy3.hash = hash;
    prom_info_copy3.prom_proof.bitmap = bitmap;
    prom_info_copy3.prom_proof.aggr_sig = tmp_bval_sig;
    prom_info_copy3.aggr_sig.bitmap = bitmap;
    prom_info_copy3.aggr_sig.aggr_sig = prom_aggr_sig;
    prom_infos_copy3.push_back(std::move(prom_info_copy3));

    AggregatedMainVoteMessagePtr valid_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_copy3));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[2],
                                         std::make_shared<ssz_types::ConsensusMessage>(valid_msg)),
        0);

    // Verify the message was processed correctly
    EXPECT_TRUE(msg_pool->IsDecided());
    EXPECT_EQ(msg_pool->decide_value_, hash);

    // Test 9: Successful reception from my_id (should skip proof verification)
    auto msg_pool_2 = myba_no_aggrbval_->GetMsgPool(ProposalKey(3, peers_[1]));
    std::vector<ssz_types::AggrPromInfo> prom_infos_copy4;
    ssz_types::AggrPromInfo prom_info_copy4;
    prom_info_copy4.hash = hash;
    prom_info_copy4.prom_proof.bitmap = bitmap;
    prom_info_copy4.prom_proof.aggr_sig = tmp_bval_sig;
    prom_info_copy4.aggr_sig.bitmap = bitmap;
    prom_info_copy4.aggr_sig.aggr_sig = prom_aggr_sig;
    prom_infos_copy4.push_back(std::move(prom_info_copy4));

    AggregatedMainVoteMessagePtr my_msg =
        CreateAggregatedMainVoteMessage(3,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_copy4));
    EXPECT_EQ(
        myba_no_aggrbval_->OnRecvMessage(peers_[0],  // my_id
                                         std::make_shared<ssz_types::ConsensusMessage>(my_msg)),
        0);
    EXPECT_TRUE(msg_pool_2->IsDecided());

    // Test 10: Receive old round message that can help decide when in higher round
    // This tests the code path at line 561: when spec_version >= 10
    // (CONSENSUS_VERSION_DECIDE_WITH_CERT) Scenario: Current node is in round 2, not decided yet,
    // receives round 0 message with pure prom

    // Create a MyBA instance with spec_version >= 10 to test the new decision proof logic
    ReliableChannelBasePtr rc_v10 = std::make_shared<MockReliableChannel>();
    auto myba_v10 = std::make_shared<MyBA>(
        10,  // spec_version >= CONSENSUS_VERSION_DECIDE_WITH_CERT (10)
        6,   // epoch
        my_id_,
        4,
        3,  // tolerable_faulty_balance out of 10 total balance
        7,  // quorum_balance out of 10 total balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        rc_v10);

    auto msg_pool_3 = myba_v10->GetMsgPool(ProposalKey(4, peers_[1]));

    // Advance to round 2 without deciding
    msg_pool_3->IncreaseRound(2);
    EXPECT_EQ(msg_pool_3->GetCurrentRound(), 2);
    EXPECT_FALSE(msg_pool_3->IsDecided());

    // Create a valid aggregated mainvote message for round 0 with pure prom (can trigger fast
    // decide)
    Digest bval_digest_test10 = CalculateBvalDigest(4,
                                                    peers_[1],
                                                    0,
                                                    hash,
                                                    myba_v10->epoch_number_,
                                                    myba_v10->spec_version_);
    Signature tmp_bval_sig_test10 = MockAggregateSign(0, bval_digest_test10);
    MockAggregateSignature(tmp_bval_sig_test10, MockAggregateSign(1, bval_digest_test10));
    MockAggregateSignature(tmp_bval_sig_test10, MockAggregateSign(2, bval_digest_test10));
    MockAggregateSignature(tmp_bval_sig_test10, MockAggregateSign(3, bval_digest_test10));

    PromMessagePtr tmp_prom_test10 = CreatePromMessage(4, peers_[1], 0, hash, AggregateSignature());
    Digest prom_digest_test10 = CalculatePromDigest(tmp_prom_test10);
    Signature prom_aggr_sig_test10 = MockAggregateSign(0, prom_digest_test10);
    MockAggregateSignature(prom_aggr_sig_test10, MockAggregateSign(1, prom_digest_test10));
    MockAggregateSignature(prom_aggr_sig_test10, MockAggregateSign(2, prom_digest_test10));
    MockAggregateSignature(prom_aggr_sig_test10, MockAggregateSign(3, prom_digest_test10));

    std::vector<ssz_types::AggrPromInfo> prom_infos_test10;
    ssz_types::AggrPromInfo prom_info_test10;
    prom_info_test10.hash = hash;
    prom_info_test10.prom_proof.bitmap = bitmap;
    prom_info_test10.prom_proof.aggr_sig = tmp_bval_sig_test10;
    prom_info_test10.aggr_sig.bitmap = bitmap;
    prom_info_test10.aggr_sig.aggr_sig = prom_aggr_sig_test10;
    prom_infos_test10.push_back(std::move(prom_info_test10));

    // Send old round (round 0) message when current round is 2
    // This should enter the code path at line 561 because:
    // 1. spec_version >= 10, so LegacyVersionBeforeDecisionProof() returns false
    // 2. aggregated_mainvote_msg->round (0) < msg_pool->GetCurrentRound() (2)
    AggregatedMainVoteMessagePtr old_round_msg =
        CreateAggregatedMainVoteMessage(4,
                                        peers_[1],
                                        0,  // round 0
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos_test10));
    EXPECT_EQ(myba_v10->OnRecvMessage(peers_[2],
                                      std::make_shared<ssz_types::ConsensusMessage>(old_round_msg)),
              0);

    // Verify that the old round message helped to decide
    EXPECT_TRUE(msg_pool_3->IsDecided());
    EXPECT_EQ(msg_pool_3->decide_value_, hash);

    // Test 11: Receive old round message with aux that matches coin and can help decide
    // This also tests the code path at line 561 with aux messages
    auto msg_pool_4 = myba_v10->GetMsgPool(ProposalKey(5, peers_[1]));

    // Advance to round 2 without deciding
    msg_pool_4->IncreaseRound(2);
    EXPECT_EQ(msg_pool_4->GetCurrentRound(), 2);
    EXPECT_FALSE(msg_pool_4->IsDecided());

    // Create aggregated mainvote with aux for round 1
    Digest bval_digest_test11 = CalculateBvalDigest(5,
                                                    peers_[1],
                                                    1,
                                                    hash,
                                                    myba_v10->epoch_number_,
                                                    myba_v10->spec_version_);
    Signature tmp_bval_sig_test11 = MockAggregateSign(0, bval_digest_test11);
    MockAggregateSignature(tmp_bval_sig_test11, MockAggregateSign(1, bval_digest_test11));
    MockAggregateSignature(tmp_bval_sig_test11, MockAggregateSign(2, bval_digest_test11));
    MockAggregateSignature(tmp_bval_sig_test11, MockAggregateSign(3, bval_digest_test11));

    AuxMessagePtr tmp_aux_test11 = CreateAuxMessage(5, peers_[1], 1, hash, AggregateSignature());
    Digest aux_digest_test11 = CalculateAuxDigest(tmp_aux_test11);
    Signature aux_aggr_sig_test11 = MockAggregateSign(0, aux_digest_test11);
    MockAggregateSignature(aux_aggr_sig_test11, MockAggregateSign(1, aux_digest_test11));
    MockAggregateSignature(aux_aggr_sig_test11, MockAggregateSign(2, aux_digest_test11));
    MockAggregateSignature(aux_aggr_sig_test11, MockAggregateSign(3, aux_digest_test11));

    std::vector<ssz_types::AggrAuxInfo> aux_infos_test11;
    ssz_types::AggrAuxInfo aux_info_test11;
    aux_info_test11.hash = hash;
    aux_info_test11.aux_proof.bitmap = bitmap;
    aux_info_test11.aux_proof.aggr_sig = tmp_bval_sig_test11;
    aux_info_test11.aggr_sig.bitmap = bitmap;
    aux_info_test11.aggr_sig.aggr_sig = aux_aggr_sig_test11;
    aux_infos_test11.push_back(std::move(aux_info_test11));

    // Send old round (round 1) message with aux when current round is 2
    AggregatedMainVoteMessagePtr old_round_aux_msg =
        CreateAggregatedMainVoteMessage(5,
                                        peers_[1],
                                        1,  // round 1
                                        std::move(aux_infos_test11),
                                        std::vector<ssz_types::AggrPromInfo>());

    // GetCoin(1) returns false (0), and the aux message has non-zero hash
    // According to line 590: else if (!coin && next_val == ZERO_32_BYTES)
    // Since coin is false but next_val is non-zero hash (not ZERO_32_BYTES), it won't decide
    // So the message will be processed but won't trigger decision, returning -3
    EXPECT_EQ(
        myba_v10->OnRecvMessage(peers_[2],
                                std::make_shared<ssz_types::ConsensusMessage>(old_round_aux_msg)),
        -3);
    EXPECT_FALSE(msg_pool_4->IsDecided());

    // Test 12: Test line 590 - coin is false and next_val is ZERO_32_BYTES, should decide
    // GetCoin(1) returns false, so we need aux with ZERO_32_BYTES to trigger decision
    auto msg_pool_5 = myba_v10->GetMsgPool(ProposalKey(6, peers_[1]));

    // Advance to round 2 without deciding
    msg_pool_5->IncreaseRound(2);
    EXPECT_EQ(msg_pool_5->GetCurrentRound(), 2);
    EXPECT_FALSE(msg_pool_5->IsDecided());

    // Add ZERO_32_BYTES to valid_values for round 1
    msg_pool_5->valid_values_[1].insert(ZERO_32_BYTES);

    // Create aggregated mainvote with aux for round 1 with ZERO_32_BYTES
    Digest bval_digest_zero = CalculateBvalDigest(6,
                                                  peers_[1],
                                                  1,
                                                  ZERO_32_BYTES,
                                                  myba_v10->epoch_number_,
                                                  myba_v10->spec_version_);
    Signature tmp_bval_sig_zero = MockAggregateSign(0, bval_digest_zero);
    MockAggregateSignature(tmp_bval_sig_zero, MockAggregateSign(1, bval_digest_zero));
    MockAggregateSignature(tmp_bval_sig_zero, MockAggregateSign(2, bval_digest_zero));
    MockAggregateSignature(tmp_bval_sig_zero, MockAggregateSign(3, bval_digest_zero));

    AuxMessagePtr tmp_aux_zero =
        CreateAuxMessage(6, peers_[1], 1, ZERO_32_BYTES, AggregateSignature());
    Digest aux_digest_zero = CalculateAuxDigest(tmp_aux_zero);
    Signature aux_aggr_sig_zero = MockAggregateSign(0, aux_digest_zero);
    MockAggregateSignature(aux_aggr_sig_zero, MockAggregateSign(1, aux_digest_zero));
    MockAggregateSignature(aux_aggr_sig_zero, MockAggregateSign(2, aux_digest_zero));
    MockAggregateSignature(aux_aggr_sig_zero, MockAggregateSign(3, aux_digest_zero));

    std::vector<ssz_types::AggrAuxInfo> aux_infos_zero;
    ssz_types::AggrAuxInfo aux_info_zero;
    aux_info_zero.hash = ZERO_32_BYTES;
    aux_info_zero.aux_proof.bitmap = bitmap;
    aux_info_zero.aux_proof.aggr_sig = tmp_bval_sig_zero;
    aux_info_zero.aggr_sig.bitmap = bitmap;
    aux_info_zero.aggr_sig.aggr_sig = aux_aggr_sig_zero;
    aux_infos_zero.push_back(std::move(aux_info_zero));

    // Send old round (round 1) message with aux (ZERO_32_BYTES) when current round is 2
    // GetCoin(1) = false, next_val = ZERO_32_BYTES, so line 590 should be triggered and decide
    AggregatedMainVoteMessagePtr old_round_zero_msg =
        CreateAggregatedMainVoteMessage(6,
                                        peers_[1],
                                        1,  // round 1
                                        std::move(aux_infos_zero),
                                        std::vector<ssz_types::AggrPromInfo>());

    EXPECT_EQ(
        myba_v10->OnRecvMessage(peers_[2],
                                std::make_shared<ssz_types::ConsensusMessage>(old_round_zero_msg)),
        0);

    // Verify that the decision was made with ZERO_32_BYTES
    EXPECT_TRUE(msg_pool_5->IsDecided());
    EXPECT_EQ(msg_pool_5->decide_value_, ZERO_32_BYTES);

    // Test 13: Test line 584 - coin is true and next_val is non-zero, should decide
    // GetCoin(2) returns true, so we need aux with non-zero hash to trigger decision
    auto msg_pool_6 = myba_v10->GetMsgPool(ProposalKey(7, peers_[1]));

    // Advance to round 3 without deciding
    msg_pool_6->IncreaseRound(3);
    EXPECT_EQ(msg_pool_6->GetCurrentRound(), 3);
    EXPECT_FALSE(msg_pool_6->IsDecided());

    // Add non-zero hash to valid_values for round 2
    msg_pool_6->valid_values_[2].insert(hash);

    // Create aggregated mainvote with aux for round 2 with non-zero hash
    Digest bval_digest_nonzero = CalculateBvalDigest(7,
                                                     peers_[1],
                                                     2,
                                                     hash,
                                                     myba_v10->epoch_number_,
                                                     myba_v10->spec_version_);
    Signature tmp_bval_sig_nonzero = MockAggregateSign(0, bval_digest_nonzero);
    MockAggregateSignature(tmp_bval_sig_nonzero, MockAggregateSign(1, bval_digest_nonzero));
    MockAggregateSignature(tmp_bval_sig_nonzero, MockAggregateSign(2, bval_digest_nonzero));
    MockAggregateSignature(tmp_bval_sig_nonzero, MockAggregateSign(3, bval_digest_nonzero));

    AuxMessagePtr tmp_aux_nonzero = CreateAuxMessage(7, peers_[1], 2, hash, AggregateSignature());
    Digest aux_digest_nonzero = CalculateAuxDigest(tmp_aux_nonzero);
    Signature aux_aggr_sig_nonzero = MockAggregateSign(0, aux_digest_nonzero);
    MockAggregateSignature(aux_aggr_sig_nonzero, MockAggregateSign(1, aux_digest_nonzero));
    MockAggregateSignature(aux_aggr_sig_nonzero, MockAggregateSign(2, aux_digest_nonzero));
    MockAggregateSignature(aux_aggr_sig_nonzero, MockAggregateSign(3, aux_digest_nonzero));

    std::vector<ssz_types::AggrAuxInfo> aux_infos_nonzero;
    ssz_types::AggrAuxInfo aux_info_nonzero;
    aux_info_nonzero.hash = hash;
    aux_info_nonzero.aux_proof.bitmap = bitmap;
    aux_info_nonzero.aux_proof.aggr_sig = tmp_bval_sig_nonzero;
    aux_info_nonzero.aggr_sig.bitmap = bitmap;
    aux_info_nonzero.aggr_sig.aggr_sig = aux_aggr_sig_nonzero;
    aux_infos_nonzero.push_back(std::move(aux_info_nonzero));

    // Send old round (round 2) message with aux (non-zero hash) when current round is 3
    // GetCoin(2) = true, next_val = hash (non-zero), so line 584 should be triggered and decide
    AggregatedMainVoteMessagePtr old_round_nonzero_msg =
        CreateAggregatedMainVoteMessage(7,
                                        peers_[1],
                                        2,  // round 2
                                        std::move(aux_infos_nonzero),
                                        std::vector<ssz_types::AggrPromInfo>());

    EXPECT_EQ(myba_v10->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(old_round_nonzero_msg)),
              0);

    // Verify that the decision was made with the non-zero hash
    EXPECT_TRUE(msg_pool_6->IsDecided());
    EXPECT_EQ(msg_pool_6->decide_value_, hash);
}

TEST_F(MyBATest, VerifyBeforeOldRoundShortcutTest) {
    // Use spec_version >= 10 (non-legacy path) where old-round shortcut exists
    ReliableChannelBasePtr rc = std::make_shared<MockReliableChannel>();
    auto myba_v10 = std::make_shared<MyBA>(
        10,  // spec_version >= CONSENSUS_VERSION_DECIDE_WITH_CERT
        6,   // epoch
        my_id_,
        4,
        3,  // tolerable_faulty_balance
        7,  // quorum_balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        rc);

    Digest hash = Digest("test_hash");
    auto msg_pool = myba_v10->GetMsgPool(ProposalKey(1, peers_[1]));

    // Advance to round 2 so incoming round-0 message enters old-round shortcut path
    msg_pool->IncreaseRound(2);
    EXPECT_FALSE(msg_pool->IsDecided());

    // Create an AggregatedMainVoteMessage with INVALID proof (wrong signatures)
    // that would match the pure-prom fast-decide path if verification were skipped
    Digest wrong_digest = Digest("wrong_data_for_sig");
    Signature bad_prom_sig = MockAggregateSign(0, wrong_digest);
    MockAggregateSignature(bad_prom_sig, MockAggregateSign(1, wrong_digest));
    MockAggregateSignature(bad_prom_sig, MockAggregateSign(2, wrong_digest));
    MockAggregateSignature(bad_prom_sig, MockAggregateSign(3, wrong_digest));

    std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
    std::string bitmap;
    myba_v10->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);

    Digest bval_digest = CalculateBvalDigest(1,
                                             peers_[1],
                                             0,
                                             hash,
                                             myba_v10->epoch_number_,
                                             myba_v10->spec_version_);
    Signature valid_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(3, bval_digest));

    std::vector<ssz_types::AggrPromInfo> prom_infos;
    ssz_types::AggrPromInfo prom_info;
    prom_info.hash = hash;
    prom_info.prom_proof.bitmap = bitmap;
    prom_info.prom_proof.aggr_sig = valid_bval_sig;
    prom_info.aggr_sig.bitmap = bitmap;
    prom_info.aggr_sig.aggr_sig = bad_prom_sig;  // invalid signature
    prom_infos.push_back(std::move(prom_info));

    AggregatedMainVoteMessagePtr forged_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,  // old round 0
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos));

    // Should return -4 (verification failure), NOT enter old-round decide path
    EXPECT_EQ(myba_v10->OnRecvMessage(peers_[2],
                                      std::make_shared<ssz_types::ConsensusMessage>(forged_msg)),
              -4);

    // Must NOT have decided with the forged message
    EXPECT_FALSE(msg_pool->IsDecided());
}

TEST_F(MyBATest, VerifyBeforeLegacyProcessingTest) {
    // Use spec_version 7 (CONSENSUS_VERSION_MIN, pre-V10 path)
    std::shared_ptr<MockReliableChannel> rc_v7 = std::make_shared<MockReliableChannel>();
    auto myba_v7 = std::make_shared<MyBA>(
        CONSENSUS_VERSION_MIN,  // spec_version 7
        6,                      // epoch
        my_id_,
        4,
        3,  // tolerable_faulty_balance
        7,  // quorum_balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        rc_v7);

    Digest hash = Digest("test_hash");
    auto msg_pool = myba_v7->GetMsgPool(ProposalKey(1, peers_[1]));

    // Create an AggregatedMainVoteMessage with INVALID proof
    Digest wrong_digest = Digest("bad_sig_data");
    Signature bad_prom_sig = MockAggregateSign(0, wrong_digest);
    MockAggregateSignature(bad_prom_sig, MockAggregateSign(1, wrong_digest));
    MockAggregateSignature(bad_prom_sig, MockAggregateSign(2, wrong_digest));
    MockAggregateSignature(bad_prom_sig, MockAggregateSign(3, wrong_digest));

    std::set<NodeId> tmp_ids{peers_[0], peers_[1], peers_[2], peers_[3]};
    std::string bitmap;
    myba_v7->crypto_helper_->GenerateBitMap(tmp_ids, bitmap);

    Digest bval_digest = CalculateBvalDigest(1, peers_[1], 0, hash);
    Signature valid_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(2, bval_digest));
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(3, bval_digest));

    std::vector<ssz_types::AggrPromInfo> prom_infos;
    ssz_types::AggrPromInfo prom_info;
    prom_info.hash = hash;
    prom_info.prom_proof.bitmap = bitmap;
    prom_info.prom_proof.aggr_sig = valid_bval_sig;
    prom_info.aggr_sig.bitmap = bitmap;
    prom_info.aggr_sig.aggr_sig = bad_prom_sig;  // invalid signature
    prom_infos.push_back(std::move(prom_info));

    AggregatedMainVoteMessagePtr forged_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,
                                        std::vector<ssz_types::AggrAuxInfo>(),
                                        std::move(prom_infos));

    // Should return -4 (verification failure) from pre-V10 path
    EXPECT_EQ(
        myba_v7->OnRecvMessage(peers_[2], std::make_shared<ssz_types::ConsensusMessage>(forged_msg)),
        -4);
    EXPECT_FALSE(msg_pool->IsDecided());
}

TEST_F(MyBATest, PhaseTwoDedupPromAuxTest) {
    // Setup: 4 nodes with balance 4,3,2,1 (total=10, quorum=7)
    // If node 0 (bal=4) sends both prom AND aux for same hash:
    //   - Old code: balance = 4 (prom) + 4 (aux) = 8 >= 7 => quorum reached (BUG)
    //   - New code: balance = 4 (deduplicated) < 7 => no quorum (CORRECT)

    std::shared_ptr<MockReliableChannel> mock_channel = std::make_shared<MockReliableChannel>();
    auto myba_test = std::make_shared<MyBA>(
        10,  // spec_version
        6,   // epoch
        my_id_,
        4,
        3,  // tolerable_faulty_balance
        7,  // quorum_balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        mock_channel);

    Digest hash = Digest("test_hash");
    NodeId proposer = peers_[1];
    auto msg_pool = myba_test->GetMsgPool(ProposalKey(1, proposer));

    // Add valid_value for round 0
    msg_pool->valid_values_[0].insert(hash);

    // Simulate node peers_[0] (balance=4) having sent BOTH prom and aux for same hash.
    // CheckPhaseTwoVoteStatus reads from aggregated_mainvote_aggr_sig_
    // (good_nodes/unverified_sigs), not from prom_/aux_ directly. Populate the correct structure.
    // Old code: balance = 4 (prom) + 4 (aux) = 8 >= 7 => quorum (BUG)
    // New code: balance = 4 (deduplicated) < 7 => no quorum (CORRECT)

    // peers_[0] appears in both Prom good_nodes AND Aux unverified_sigs for same hash/round
    msg_pool->aggregated_mainvote_aggr_sig_[0][hash][AggregatedMainVoteType::Prom]
        .good_nodes.insert(peers_[0]);
    msg_pool->aggregated_mainvote_aggr_sig_[0][hash][AggregatedMainVoteType::Aux]
        .unverified_sigs[peers_[0]] = "dummy_sig";

    // Also need prom/aux entries so TryGenerateMainVoteAggrSignature can work
    Digest bval_digest = CalculateBvalDigest(1,
                                             proposer,
                                             0,
                                             hash,
                                             myba_test->epoch_number_,
                                             myba_test->spec_version_);
    PromMessagePtr prom = CreatePromMessage(1, proposer, 0, hash, AggregateSignature());
    msg_pool->prom_[0][hash][peers_[0]] =
        Signature(MockAggregateSign(0, CalculatePromDigest(prom)));
    AuxMessagePtr aux = CreateAuxMessage(1, proposer, 0, hash, AggregateSignature());
    msg_pool->aux_[0][hash][peers_[0]] = aux;

    // Call CheckPhaseTwoVoteStatus — should NOT reach quorum because peers_[0] balance=4 < 7
    // Before the fix (double-counting), balance would be 4+4=8 >= 7, incorrectly reaching quorum.
    // After the fix (dedup), balance is 4 < 7, correctly rejecting.
    myba_test->CheckPhaseTwoVoteStatus(msg_pool, 0, hash);

    // AggregatedMainVote should NOT have been generated (quorum not met after dedup)
    EXPECT_FALSE(msg_pool->ExistAggregatedMainVote(0));
}

TEST_F(MyBATest, RejectsMultiHashMainVoteWithGarbageInnerProofTest) {
    // Attack scenario:
    //   - peers_[3] (balance=1) is the malicious node
    //   - Honest nodes peers_[0](4) + peers_[1](3) + peers_[2](2) signed prom for hash (balance=9)
    //   - Malicious peers_[3] injects aux for ZERO_32_BYTES with valid outer signature
    //     but GARBAGE aux_proof (no real bval QC for ZERO_32_BYTES)
    //   - Total balance = 9+1 = 10 >= 7 (quorum), so VerifyAggregatedMainVote passes
    //   - valid_values ends up containing both hash AND ZERO_32_BYTES
    //   - When coin is false, the decision logic picks ZERO_32_BYTES — attack succeeds
    //
    // If inner proof were verified, the garbage aux_proof would be rejected and
    // ZERO_32_BYTES would NOT enter valid_values.

    ReliableChannelBasePtr rc = std::make_shared<MockReliableChannel>();
    auto myba_v10 = std::make_shared<MyBA>(
        10,  // spec_version >= CONSENSUS_VERSION_DECIDE_WITH_CERT
        6,   // epoch
        my_id_,
        4,
        3,  // tolerable_faulty_balance
        7,  // quorum_balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        rc);

    Digest hash = Digest("test_hash");
    auto msg_pool = myba_v10->GetMsgPool(ProposalKey(1, peers_[1]));

    // Node is at round 0. Send a round-1 message to trigger the future-round path (line 649).
    EXPECT_EQ(msg_pool->GetCurrentRound(), 0);

    // --- Honest prom entry: peers_[0]+peers_[1]+peers_[2] sign prom for hash (balance=9) ---
    std::set<NodeId> honest_ids{peers_[0], peers_[1], peers_[2]};
    std::string honest_bitmap;
    myba_v10->crypto_helper_->GenerateBitMap(honest_ids, honest_bitmap);

    PromMessagePtr tmp_prom = CreatePromMessage(1, peers_[1], 1, hash, AggregateSignature());
    Digest prom_digest = CalculatePromDigest(tmp_prom);
    Signature prom_aggr_sig = MockAggregateSign(0, prom_digest);
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(1, prom_digest));
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(2, prom_digest));

    // Valid bval QC for the prom inner proof (honest nodes actually did bval for hash)
    Digest bval_digest = CalculateBvalDigest(1,
                                             peers_[1],
                                             1,
                                             hash,
                                             myba_v10->epoch_number_,
                                             myba_v10->spec_version_);
    Signature valid_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(2, bval_digest));

    ssz_types::AggrPromInfo prom_info;
    prom_info.hash = hash;
    prom_info.prom_proof.bitmap = honest_bitmap;
    prom_info.prom_proof.aggr_sig = valid_bval_sig;  // legitimate bval QC
    prom_info.aggr_sig.bitmap = honest_bitmap;
    prom_info.aggr_sig.aggr_sig = prom_aggr_sig;

    // --- Malicious aux entry: peers_[3] signs aux for ZERO_32_BYTES (balance=1) ---
    // The outer signature is correctly computed, but aux_proof is garbage —
    // there was never a bval quorum for ZERO_32_BYTES.
    std::set<NodeId> malicious_ids{peers_[3]};
    std::string malicious_bitmap;
    myba_v10->crypto_helper_->GenerateBitMap(malicious_ids, malicious_bitmap);

    AuxMessagePtr tmp_aux =
        CreateAuxMessage(1, peers_[1], 1, ZERO_32_BYTES, AggregateSignature());
    Digest aux_digest = CalculateAuxDigest(tmp_aux);
    Signature aux_aggr_sig = MockAggregateSign(3, aux_digest);  // only malicious node

    // Garbage inner proof: random data, not a valid bval aggregate for ZERO_32_BYTES
    Digest garbage_digest = Digest("garbage_not_a_real_bval_digest");
    Signature garbage_bval_sig = MockAggregateSign(3, garbage_digest);

    ssz_types::AggrAuxInfo aux_info;
    aux_info.hash = ZERO_32_BYTES;
    aux_info.aux_proof.bitmap = malicious_bitmap;
    aux_info.aux_proof.aggr_sig = garbage_bval_sig;  // GARBAGE: no real bval QC
    aux_info.aggr_sig.bitmap = malicious_bitmap;
    aux_info.aggr_sig.aggr_sig = aux_aggr_sig;  // valid outer signature by malicious node

    std::vector<ssz_types::AggrPromInfo> prom_infos;
    prom_infos.push_back(std::move(prom_info));
    std::vector<ssz_types::AggrAuxInfo> aux_infos;
    aux_infos.push_back(std::move(aux_info));

    AggregatedMainVoteMessagePtr future_round_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        1,  // round 1 (future for current round 0)
                                        std::move(aux_infos),
                                        std::move(prom_infos));

    // VerifyAggregatedMainVote now detects multiple distinct hashes and verifies
    // inner proofs. The garbage aux_proof for ZERO_32_BYTES fails VerifyPhaseOneQC,
    // so the message is rejected with -4.
    EXPECT_EQ(myba_v10->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(future_round_msg)),
              -4);

    // Round must NOT advance — the malicious message was rejected.
    EXPECT_EQ(msg_pool->GetCurrentRound(), 0);
}

TEST_F(MyBATest, RejectsMultiHashMainVoteWithSubQuorumInnerProofTest) {
    // Attack scenario:
    //   - peers_[3] (balance=1) is the malicious node
    //   - Honest nodes peers_[0](4) + peers_[1](3) + peers_[2](2) signed prom for hash (balance=9)
    //   - Malicious peers_[3] creates a VALID bval signature for ZERO_32_BYTES (only 1 signer,
    //     balance=1, below quorum=7) and uses it as aux_proof in an aux entry
    //   - The inner proof signature is cryptographically valid but lacks quorum balance
    //   - Total outer balance = 9+1 = 10 >= 7 (quorum), so outer check passes
    //
    // Without the CheckQuorumBalance fix, this would pass VerifyAggregatedMainVote
    // because VerifyPhaseOneQC only checks signature validity (threshold=1), not balance.
    // With the fix, the sub-quorum inner proof is rejected.

    ReliableChannelBasePtr rc = std::make_shared<MockReliableChannel>();
    auto myba_v10 = std::make_shared<MyBA>(
        10,  // spec_version >= CONSENSUS_VERSION_DECIDE_WITH_CERT
        6,   // epoch
        my_id_,
        4,
        3,  // tolerable_faulty_balance
        7,  // quorum_balance
        0,  // stable seq
        engine_->crypto_helper_,
        peer_pubkey_map_,
        peer_balance_map_,
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            endorsed_[seq][proposer] = hash;
        },
        [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {},
        [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
            myba_completed_[seq][proposer] = hash;
        },
        rc);

    Digest hash = Digest("test_hash");
    auto msg_pool = myba_v10->GetMsgPool(ProposalKey(1, peers_[1]));
    EXPECT_EQ(msg_pool->GetCurrentRound(), 0);

    // --- Honest prom entry: peers_[0]+peers_[1]+peers_[2] sign prom for hash (balance=9) ---
    std::set<NodeId> honest_ids{peers_[0], peers_[1], peers_[2]};
    std::string honest_bitmap;
    myba_v10->crypto_helper_->GenerateBitMap(honest_ids, honest_bitmap);

    PromMessagePtr tmp_prom = CreatePromMessage(1, peers_[1], 0, hash, AggregateSignature());
    Digest prom_digest = CalculatePromDigest(tmp_prom);
    Signature prom_aggr_sig = MockAggregateSign(0, prom_digest);
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(1, prom_digest));
    MockAggregateSignature(prom_aggr_sig, MockAggregateSign(2, prom_digest));

    // Valid bval QC for the prom inner proof (honest nodes)
    Digest bval_digest = CalculateBvalDigest(1,
                                             peers_[1],
                                             0,
                                             hash,
                                             myba_v10->epoch_number_,
                                             myba_v10->spec_version_);
    Signature valid_bval_sig = MockAggregateSign(0, bval_digest);
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(1, bval_digest));
    MockAggregateSignature(valid_bval_sig, MockAggregateSign(2, bval_digest));

    ssz_types::AggrPromInfo prom_info;
    prom_info.hash = hash;
    prom_info.prom_proof.bitmap = honest_bitmap;
    prom_info.prom_proof.aggr_sig = valid_bval_sig;  // legitimate bval QC
    prom_info.aggr_sig.bitmap = honest_bitmap;
    prom_info.aggr_sig.aggr_sig = prom_aggr_sig;

    // --- Malicious aux entry: peers_[3] signs aux for ZERO_32_BYTES (balance=1) ---
    // The outer signature is correctly computed. The aux_proof is a VALID bval signature
    // from peers_[3] only (balance=1, sub-quorum). This passes VerifyPhaseOneQC's
    // signature check but should fail CheckQuorumBalance.
    std::set<NodeId> malicious_ids{peers_[3]};
    std::string malicious_bitmap;
    myba_v10->crypto_helper_->GenerateBitMap(malicious_ids, malicious_bitmap);

    AuxMessagePtr tmp_aux =
        CreateAuxMessage(1, peers_[1], 0, ZERO_32_BYTES, AggregateSignature());
    Digest aux_digest = CalculateAuxDigest(tmp_aux);
    Signature aux_aggr_sig = MockAggregateSign(3, aux_digest);  // only malicious node

    // Sub-quorum inner proof: valid bval signature from peers_[3] only (balance=1 < quorum=7)
    Digest bval_digest_zero = CalculateBvalDigest(1,
                                                  peers_[1],
                                                  0,
                                                  ZERO_32_BYTES,
                                                  myba_v10->epoch_number_,
                                                  myba_v10->spec_version_);
    Signature sub_quorum_bval_sig = MockAggregateSign(3, bval_digest_zero);  // valid but sub-quorum

    ssz_types::AggrAuxInfo aux_info;
    aux_info.hash = ZERO_32_BYTES;
    aux_info.aux_proof.bitmap = malicious_bitmap;
    aux_info.aux_proof.aggr_sig = sub_quorum_bval_sig;  // valid signature, but only 1 signer
    aux_info.aggr_sig.bitmap = malicious_bitmap;
    aux_info.aggr_sig.aggr_sig = aux_aggr_sig;

    std::vector<ssz_types::AggrPromInfo> prom_infos;
    prom_infos.push_back(std::move(prom_info));
    std::vector<ssz_types::AggrAuxInfo> aux_infos;
    aux_infos.push_back(std::move(aux_info));

    // Use round 0 (current round) to test that the fix also covers same-round messages
    AggregatedMainVoteMessagePtr same_round_msg =
        CreateAggregatedMainVoteMessage(1,
                                        peers_[1],
                                        0,  // round 0 (current round, not future)
                                        std::move(aux_infos),
                                        std::move(prom_infos));

    // With CheckQuorumBalance fix: sub-quorum inner proof is rejected with -4
    EXPECT_EQ(myba_v10->OnRecvMessage(
                  peers_[2],
                  std::make_shared<ssz_types::ConsensusMessage>(same_round_msg)),
              -4);

    // Round must NOT advance
    EXPECT_EQ(msg_pool->GetCurrentRound(), 0);
    // Must NOT decide
    EXPECT_FALSE(msg_pool->IsDecided());
}

TEST_F(MyBATest, GetCoinTest) {
    Round round = 0;
    int one_count = 0;
    int zero_count = 0;
    do {
        if (myba_no_aggrbval_->GetCoin(round)) {
            ++one_count;
        } else {
            ++zero_count;
        }
        ++round;
    } while (round != 0);
    EXPECT_EQ(one_count, 127);
    EXPECT_EQ(zero_count, 129);
    unsigned int int_round = 0;
    one_count = 0;
    zero_count = 0;
    while (int_round < 512) {
        if (myba_no_aggrbval_->GetCoin(int_round)) {
            ++one_count;
        } else {
            ++zero_count;
        }
        ++int_round;
    }
    EXPECT_EQ(one_count, 254);
    EXPECT_EQ(zero_count, 258);
}
}  // namespace consensus_spec
