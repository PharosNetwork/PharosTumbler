// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <iostream>
#include "test/protocol/myba_state_machine_fixture.h"
#include "test/protocol/check_myba_state.h"

#include "consensus/protocol/myba.h"

namespace consensus_spec {
using consensus_spec::ssz_types::AggregateSignature;

#define NOWTIME                                                                                  \
    static_cast<long long unsigned int>(std::chrono::duration_cast<std::chrono::milliseconds>(   \
                                            std::chrono::system_clock::now().time_since_epoch()) \
                                            .count())

class MyBAStateMachineTest : public testing::TestWithParam<bool>,
                             public myba_state_machine_fixture {
  public:
    MyBAStateMachineTest() : myba_state_machine_fixture() {
        myba_state.seq_ = 1;
        myba_state.proposer_index_ = 1;
        myba_state.proposer_node_ = peers_[myba_state.proposer_index_];
        myba_state.rc_ = std::make_shared<MockReliableChannel>();
        myba_state.proposal_hash_ = proposal_hash_map_[myba_state.seq_][myba_state.proposer_node_];
        myba_state.myba_no_aggrbval_ = std::make_shared<MyBA>(
            myba_state.spec_version_,
            0,  // epoch
            my_id_,
            7,  // n
            2,  // tolerable_faulty_balance out of 7 total balance
            5,  // quorum_balance out of 7 total balance
            0,  // stable seq
            engine_->crypto_helper_,
            peer_pubkey_map_,
            peer_balance_map_,
            [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
                myba_state.endorsed_[seq][proposer] = hash;
            },
            [this](const Seq seq, const NodeId& node_id, const Digest& hash, const NodeId& sender) {
            },
            [this](const Seq& seq, const NodeId& proposer, const Digest& hash) {
                myba_state.myba_completed_[seq][proposer] = hash;
            },
            myba_state.rc_);
    }

    enum MsgType {
        Bval = 0,
        Prom = 1,
        Aux = 2
    };

    const Digest getProposalHash(bool val) {
        return val ? proposal_hash_map_[myba_state.seq_][myba_state.proposer_node_] : ZERO_32_BYTES;
    }

    void Receive2fA1Bval(bool bvalx, Round round) {
        ReceiveKNumBvalx(5, bvalx, round);
    }

    void Receive2fBval(bool bvalx, Round round) {
        ReceiveKNumBvalx(4, bvalx, round);
    }

    void ReceiveKNumBvalx(int k, bool bvalx, Round round) {
        std::vector<uint32_t> node_index;
        for (int i = 1; i <= k; ++i) {
            node_index.push_back(i);
        }
        ReceiveBvalx(node_index, bvalx, round);
    }

    void ReceiveBvalx(std::vector<uint32_t> node_index, bool bvalx, Round round) {
        Digest bval_hash = getProposalHash(bvalx);
        auto bval_msgs = CreateBvalMessageByBtach(myba_state.seq_,
                                                  node_index,
                                                  round,
                                                  myba_state.proposer_index_,
                                                  bval_hash,
                                                  true,
                                                  myba_state.epoch_number_,
                                                  myba_state.spec_version_);
        for (uint32_t i = 0; i < node_index.size(); ++i) {
            auto msg = std::make_shared<ssz_types::ConsensusMessage>(bval_msgs[i]);
            EXPECT_EQ(myba_state.myba_no_aggrbval_->OnRecvMessage(peers_[node_index[i]], msg), 0);
        }
    }

    void ReceiveProm(bool px, Round round) {
        ReceiveKNumPromx(1, px, round);
    }

    void Receive2fProm(bool px, Round round) {
        ReceiveKNumPromx(4, px, round);
    }

    void ReceiveKNumPromx(uint32_t k, bool px, Round round) {
        std::vector<uint32_t> node_index;
        for (uint32_t i = 1; i <= k; ++i) {
            node_index.push_back(i);
        }
        Digest p_hash = getProposalHash(px);
        auto prom_msgs = CreatePromMessageByBatch(myba_state.seq_,
                                                  node_index,
                                                  round,
                                                  myba_state.proposer_index_,
                                                  p_hash,
                                                  myba_state.epoch_number_,
                                                  myba_state.spec_version_);
        for (uint32_t i = 0; i < node_index.size(); ++i) {
            auto msg = std::make_shared<ssz_types::ConsensusMessage>(prom_msgs[i]);
            EXPECT_EQ(myba_state.myba_no_aggrbval_->OnRecvMessage(peers_[node_index[i]], msg), 0);
        }
    }

    void Receive1Aux(bool ax, Round round) {
        ReceiveKNumAuxx(1, ax, round);
    }

    void Receive2fAux(bool ax, Round round) {
        ReceiveKNumAuxx(4, ax, round);
    }

    void ReceiveKNumAuxx(int k, bool valx, Round round) {
        std::vector<uint32_t> node_index;
        for (int i = 1; i <= k; ++i) {
            node_index.push_back(i);
        }
        ReceiveAux(node_index, valx, round);
    }

    void ReceiveAux(std::vector<uint32_t> node_index, bool valx, Round round) {
        Digest a_hash = getProposalHash(valx);
        auto aux_msgs = CreateAuxMessageByBatch(myba_state.seq_,
                                                node_index,
                                                round,
                                                myba_state.proposer_index_,
                                                a_hash,
                                                myba_state.epoch_number_,
                                                myba_state.spec_version_);
        for (uint32_t i = 0; i < node_index.size(); ++i) {
            auto msg = std::make_shared<ssz_types::ConsensusMessage>(aux_msgs[i]);
            EXPECT_EQ(myba_state.myba_no_aggrbval_->OnRecvMessage(peers_[node_index[i]], msg), 0);
        }
    }

    void Receive2fPromAndAux(bool px, Round round) {
        ReceiveKNumPromx(2, px, round);

        std::vector<uint32_t> node_index = {3, 4};
        ReceiveAux(node_index, px, round);
    }

    void Receive2fPromAndAuxMixed(Round round) {
        ReceiveKNumPromx(2, false, round);

        std::vector<uint32_t> node_index = {3, 4};
        ReceiveAux(node_index, true, round);
    }

    // State transition helpers.
    void TransitToA2(bool bvalx) {
        ReceiveKNumBvalx(3, bvalx, round0_);
    }

    void TransitToA3orA5(bool is_a3) {
        ReceiveKNumBvalx(4, is_a3, round0_);
    }

    void TransitToA4(bool bval2f = true) {
        TransitToA3orA5(!bval2f);
        std::vector<uint32_t> node_index = {5};
        ReceiveBvalx(node_index, bval2f, round0_);
    }

    void TransitToB1() {
        myba_state.myba_no_aggrbval_->DoEndorse(1,
                                                peers_[1],
                                                proposal_hash_map_[1][peers_[1]],
                                                true);
    }

    void TransitToB2() {
        TransitToB1();
        myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    }

    void TransitToB3() {
        myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    }

    void TransitToC1() {
        // Based on fromA1ToC1 which calls Receive2fA1Bval(true)
        Receive2fA1Bval(true, round0_);
        EXPECT_TRUE(myba_state.CheckMyBAStateC1());
    }

    void TransitToC2() {
        TransitToC1();
        myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
        EXPECT_TRUE(myba_state.CheckMyBAStateC2());
    }

    void TransitToC3() {
        TransitToB2();
        // 2fbval1, b1 has 1 bval1.
        Receive2fBval(true, round0_);
        EXPECT_TRUE(myba_state.CheckMyBAStateC3());
    }

    void TransitToC4() {
        // Based on fromA1ToC4 which goes B2 then Receive2fA1Bval(false)
        TransitToB2();                        // First reach B2 state
        ReceiveKNumBvalx(4, false, round0_);  // 2f+1 = 5 nodes with bval=false
        EXPECT_TRUE(myba_state.CheckMyBAStateC4());
    }

    void TransitToC5() {
        // Based on fromA1ToC5 which goes B3 then Receive2fA1Bval(false)
        TransitToB3();                        // First reach B3 state
        ReceiveKNumBvalx(4, false, round0_);  // 2f+1 = 5 nodes with bval=false
        EXPECT_TRUE(myba_state.CheckMyBAStateC5());
    }

    void TransitToD1(bool promx = true) {
        Receive2fProm(promx, round0_);
        EXPECT_TRUE(myba_state.CheckMyBAStateD1());
    }

    void TransitToD2() {
        Receive2fPromAndAux(true, round0_);
        EXPECT_TRUE(myba_state.CheckMyBAStateD2());
    }

    void TransitToD3() {
        Receive2fPromAndAux(false, round0_);
        EXPECT_TRUE(myba_state.CheckMyBAStateD3());
    }

    void TransitToD4() {
        Receive2fPromAndAuxMixed(round0_);
        EXPECT_TRUE(myba_state.CheckMyBAStateD4());
    }

    MyBAState myba_state;

  private:
    Round round0_ = 0;
};

TEST_F(MyBAStateMachineTest, TestA1ToB1ToB2) {
    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], proposal_hash_map_[1][peers_[1]], true);
    EXPECT_TRUE(myba_state.CheckMyBAStateB1());

    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateB2());
}

TEST_F(MyBAStateMachineTest, TestA1ToB3_bySwitchToNormalPath) {
    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateB3());
}

TEST_F(MyBAStateMachineTest, TestA1ToB3_byEndorse0) {
    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], ZERO_32_BYTES, true);
    EXPECT_TRUE(myba_state.CheckMyBAStateB3());
}

TEST_P(MyBAStateMachineTest, TestA2ToB1ToB2) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);

    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], proposal_hash_map_[1][peers_[1]], true);
    EXPECT_TRUE(myba_state.CheckMyBAStateB1());

    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateB2());
}

TEST_P(MyBAStateMachineTest, TestA2ToB3) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);

    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateB3());
}

TEST_F(MyBAStateMachineTest, TestA1ToD1) {
    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestA1ToD2) {
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestA1ToD3) {
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestA1ToD4) {
    TransitToD4();
}

TEST_P(MyBAStateMachineTest, TestA2ToD1) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);
    TransitToD1();
}

TEST_P(MyBAStateMachineTest, TestA2ToD2) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);
    TransitToD2();
}

TEST_P(MyBAStateMachineTest, TestA2ToD3) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);
    TransitToD3();
}

TEST_P(MyBAStateMachineTest, TestA2ToD4) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestA3ToD1) {
    TransitToA3orA5(true);
    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestA3ToD2) {
    TransitToA3orA5(true);
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestA3ToD3) {
    TransitToA3orA5(true);
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestA3ToD4) {
    TransitToA3orA5(true);
    TransitToD4();
}

TEST_P(MyBAStateMachineTest, TestA4ToD1) {
    TransitToA4(GetParam());
    TransitToD1();
}

TEST_P(MyBAStateMachineTest, TestA4ToD2) {
    TransitToA4(GetParam());
    TransitToD2();
}

TEST_P(MyBAStateMachineTest, TestA4ToD3) {
    TransitToA4(GetParam());
    TransitToD3();
}

TEST_P(MyBAStateMachineTest, TestA4ToD4) {
    TransitToA4(GetParam());
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestA5ToD1) {
    TransitToA3orA5(false);

    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestA5ToD2) {
    TransitToA3orA5(false);
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestA5ToD3) {
    TransitToA3orA5(false);
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestA5ToD4) {
    TransitToA3orA5(false);
    TransitToD4();
}

// Replace the existing Test*ToD* functions with expanded versions

TEST_F(MyBAStateMachineTest, TestB1ToD1) {
    TransitToB1();
    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestB2ToD1) {
    TransitToB2();
    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestB3ToD1) {
    TransitToB3();
    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestB1ToD2) {
    TransitToB1();
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestB2ToD2) {
    TransitToB2();
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestB3ToD2) {
    TransitToB3();
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestB1ToD3) {
    TransitToB1();
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestB2ToD3) {
    TransitToB2();
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestB3ToD3) {
    TransitToB3();
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestB1ToD4) {
    TransitToB1();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestB2ToD4) {
    TransitToB2();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestB3ToD4) {
    TransitToB3();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestC1ToD1) {
    TransitToC1();
    TransitToD1();
}

TEST_F(MyBAStateMachineTest, TestC2ToD1) {
    TransitToC2();
    TransitToD1();
}

// need to handle 2f+1 prom1 when aux0/prom0
// TEST_F(MyBAStateMachineTest, TestC3ToD1) {
//     TransitToC3();
//     TransitToD1();
// }

// TEST_F(MyBAStateMachineTest, TestC4ToD1) {
//     TransitToC4();
//     TransitToD1();
// }

TEST_F(MyBAStateMachineTest, TestC5ToD1) {
    TransitToC5();
    TransitToD1(false);
}

TEST_F(MyBAStateMachineTest, TestC1ToD2) {
    TransitToC1();
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestC2ToD2) {
    TransitToC2();
    TransitToD2();
}

TEST_F(MyBAStateMachineTest, TestC3ToD2) {
    TransitToC3();
    TransitToD2();
}

// TEST_F(MyBAStateMachineTest, TestC4ToD2) {
//     TransitToC4();
//     TransitToD2();
// }

// TEST_F(MyBAStateMachineTest, TestC5ToD2) {
//     TransitToC5();
//     TransitToD2();
// }

// TEST_F(MyBAStateMachineTest, TestC1ToD3) {
//     TransitToC1();
//     TransitToD3();
// }

// TEST_F(MyBAStateMachineTest, TestC2ToD3) {
//     TransitToC2();
//     TransitToD3();
// }

// TEST_F(MyBAStateMachineTest, TestC3ToD3) {
//     TransitToC3();
//     TransitToD3();
// }

TEST_F(MyBAStateMachineTest, TestC4ToD3) {
    TransitToC4();
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestC5ToD3) {
    TransitToC5();
    TransitToD3();
}

TEST_F(MyBAStateMachineTest, TestC1ToD4) {
    TransitToC1();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestC2ToD4) {
    TransitToC2();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestC3ToD4) {
    TransitToC3();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestC4ToD4) {
    TransitToC4();
    TransitToD4();
}

TEST_F(MyBAStateMachineTest, TestC5ToD4) {
    TransitToC5();
    TransitToD4();
}

TEST_P(MyBAStateMachineTest, TestA1ToA2) {
    // Cumulative bval x = true or false.
    bool bvalx = GetParam();
    Digest bval_hash = bvalx ? proposal_hash_map_[1][peers_[1]] : ZERO_32_BYTES;
    std::vector<uint32_t> node_index = {1, 2, 3};
    auto bval_msgs = CreateBvalMessageByBtach(1, node_index, 0, 1, bval_hash, false, 0, 7);
    // Receive f bval 1 first.
    for (uint32_t i = 0; i < node_index.size() - 1; ++i) {
        auto msg = std::make_shared<ssz_types::ConsensusMessage>(bval_msgs[i]);
        EXPECT_EQ(myba_state.myba_no_aggrbval_->OnRecvMessage(peers_[node_index[i]], msg), 0);
    }
    EXPECT_EQ(myba_state.endorsed_[1].count(peers_[1]), 0);
    // Receive f+1 bval 1.
    auto msg = std::make_shared<ssz_types::ConsensusMessage>(bval_msgs.back());
    EXPECT_EQ(myba_state.myba_no_aggrbval_->OnRecvMessage(peers_[node_index.back()], msg), 0);
    // EndorseCallback should be called.
    EXPECT_EQ(myba_state.endorsed_[1].count(peers_[1]), 1);
    EXPECT_EQ(myba_state.endorsed_[1][peers_[1]], Digest(bval_hash));
    EXPECT_TRUE(myba_state.CheckMyBAStateA2(bvalx));
}

TEST_F(MyBAStateMachineTest, TestA1ToA3) {
    ReceiveKNumBvalx(4, true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateA3());
}

TEST_F(MyBAStateMachineTest, TestA1ToA5) {
    ReceiveKNumBvalx(4, false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateA5());
}

TEST_P(MyBAStateMachineTest, TestA2ToA3) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);
    std::vector<uint32_t> node_index = {4};
    if (!bvalx) {
        return;  // Can't reach A3.
    }
    ReceiveBvalx(node_index, bvalx, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateA3());
}

TEST_P(MyBAStateMachineTest, TestA2ToA5) {
    bool bvalx = GetParam();
    TransitToA2(bvalx);
    std::vector<uint32_t> node_index = {4};
    if (bvalx) {
        return;  // Can't reach A5.
    }
    ReceiveBvalx(node_index, bvalx, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateA5());
}

TEST_F(MyBAStateMachineTest, TestA3ToA4) {
    ReceiveKNumBvalx(4, true, round0_);

    std::vector<uint32_t> node_index = {5};
    ReceiveBvalx(node_index, false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateA4());
}

TEST_F(MyBAStateMachineTest, TestA5ToA4) {
    ReceiveKNumBvalx(4, false, round0_);

    std::vector<uint32_t> node_index = {5};
    ReceiveBvalx(node_index, true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateA4());
}

TEST_F(MyBAStateMachineTest, TestA1ToC1_byBval) {
    // 2f + 1 bval1.
    Receive2fA1Bval(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA1ToC1_byProm) {
    // Prom 1
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA1ToC1_byAux) {
    // Aux 1
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA2ToC1_byProm) {
    // Prom 1
    TransitToA2(true);
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA2ToC1_byAux) {
    // Aux 1
    TransitToA2(true);
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA3ToC1_byProm) {
    TransitToA3orA5(true);
    // Prom 1
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_P(MyBAStateMachineTest, TestA3ToC1_byAux) {
    TransitToA3orA5(true);
    // Aux 1
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA3ToC1_byEndorse) {
    TransitToA3orA5(true);
    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], proposal_hash_map_[1][peers_[1]], true);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA4ToC1_byEndorse) {
    TransitToA4(false);
    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], proposal_hash_map_[1][peers_[1]], true);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_P(MyBAStateMachineTest, TestA4ToC1_byProm) {
    bool bx = GetParam();
    TransitToA4(bx);
    // Prom 1
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_P(MyBAStateMachineTest, TestA4ToC1_byAux) {
    bool bx = GetParam();
    TransitToA4(bx);
    // Aux 1
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestA1ToC5_byBval) {
    // 2f + 1 bval0.
    Receive2fA1Bval(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA1ToC5_byProm) {
    // Prom 0
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA1ToC5_byAux) {
    // Aux 0
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA2ToC5_byProm) {
    // Prom 0
    TransitToA2(false);
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA2ToC5_byAux) {
    // Aux 0
    TransitToA2(false);
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA5ToC5_byProm) {
    TransitToA3orA5(false);
    // Prom 0
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA5ToC5_byAux) {
    TransitToA3orA5(false);
    // Aux 0
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA4ToC5_byEndorse) {
    TransitToA4(true);
    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], ZERO_32_BYTES, true);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA4ToC5_bySwitchNormalPath) {
    TransitToA4(true);
    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_P(MyBAStateMachineTest, TestA4ToC5_byProm) {
    bool bx = GetParam();
    TransitToA4(bx);
    // Prom 0
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_P(MyBAStateMachineTest, TestA4ToC5_byAux) {
    bool bx = GetParam();
    TransitToA4(bx);
    // Aux 0
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA5ToC5_byEndorse) {
    TransitToA3orA5(false);
    myba_state.myba_no_aggrbval_->DoEndorse(1, peers_[1], ZERO_32_BYTES, true);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestA5ToC5_bySwitchNormalPath) {
    TransitToA3orA5(false);
    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestC1ToC2) {
    // 2f + 1 bval1.
    Receive2fA1Bval(true, round0_);

    myba_state.myba_no_aggrbval_->DoSwitchToNormalPath(1, peers_[1]);
    EXPECT_TRUE(myba_state.CheckMyBAStateC2());
}

TEST_F(MyBAStateMachineTest, TestB1ToC1_byBval) {
    TransitToB1();
    // 2f + 1 bval1.
    Receive2fBval(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestB1ToC1_byProm) {
    TransitToB1();
    // Prom 1.
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestB1ToC1_byAux) {
    TransitToB1();
    // Aux 1.
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC1());
}

TEST_F(MyBAStateMachineTest, TestB1ToC4_byBval) {
    TransitToB1();
    // 2f bval0
    Receive2fA1Bval(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC4());
}

TEST_F(MyBAStateMachineTest, TestB1ToC4_byProm) {
    TransitToB1();
    // Prom 0
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC4());
}

TEST_F(MyBAStateMachineTest, TestB1ToC4_byAux) {
    TransitToB1();
    // Aux 0
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC4());
}

TEST_F(MyBAStateMachineTest, TestB2ToC3_byBval) {
    TransitToB2();
    // 2fbval1.
    Receive2fBval(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC3());
    auto proposal = ProposalKey(1, peers_[1]);
    auto msg_pool = myba_state.myba_no_aggrbval_->GetMsgPool(proposal);
    EXPECT_TRUE(msg_pool->IsNormalPath());
    EXPECT_TRUE(msg_pool->ExistAux(proposal_hash_map_[1][peers_[1]], 0, peers_[0]));
}

TEST_F(MyBAStateMachineTest, TestB2ToC3_byProm) {
    TransitToB2();
    // Prom 1
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC3());
}

TEST_F(MyBAStateMachineTest, TestB2ToC3_byAux) {
    TransitToB2();
    // Aux 1
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC3());
}

TEST_F(MyBAStateMachineTest, TestB2ToC4_byBval) {
    TransitToB2();
    // 2f bval0.
    Receive2fBval(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC4());
}

TEST_F(MyBAStateMachineTest, TestB2ToC4_byProm) {
    TransitToB2();
    // Prom 0
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC4());
}

TEST_F(MyBAStateMachineTest, TestB2ToC4_byAux) {
    TransitToB2();
    // Aux 0
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC4());
}

TEST_F(MyBAStateMachineTest, TestB3ToC3_byBval) {
    TransitToB3();
    // 2f + 1 bval1.
    Receive2fA1Bval(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC3());
    auto proposal = ProposalKey(1, peers_[1]);
    auto msg_pool = myba_state.myba_no_aggrbval_->GetMsgPool(proposal);
    EXPECT_TRUE(msg_pool->IsNormalPath());
    EXPECT_TRUE(msg_pool->ExistAux(proposal_hash_map_[1][peers_[1]], 0, peers_[0]));
}

TEST_F(MyBAStateMachineTest, TestB3ToC3_byProm) {
    TransitToB3();
    // Prom 1
    ReceiveProm(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC3());
}

TEST_F(MyBAStateMachineTest, TestB3ToC3_byAux) {
    TransitToB3();
    // Aux 1
    Receive1Aux(true, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC3());
}

TEST_F(MyBAStateMachineTest, TestB3ToC5_byBval) {
    TransitToB3();
    // 2f bval0.
    Receive2fBval(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestB3ToC5_byProm) {
    TransitToB3();
    // Prom 0
    ReceiveProm(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

TEST_F(MyBAStateMachineTest, TestB3ToC5_byAux) {
    TransitToB3();
    // Aux 0
    Receive1Aux(false, round0_);
    EXPECT_TRUE(myba_state.CheckMyBAStateC5());
}

INSTANTIATE_TEST_SUITE_P(BvalX, MyBAStateMachineTest, testing::Values(false, true));

class MyBAStateMachineTestRound1 : public MyBAStateMachineTest {
  public:
    MyBAStateMachineTestRound1() : MyBAStateMachineTest() {}

    void SetUp() override {
        coin1_ = myba_state.myba_no_aggrbval_->GetCoin(round1_);
    }

    void D4ToE1() {
        TransitToD4();
        EXPECT_TRUE(myba_state.CheckMyBAStateE1());
    }

    void E1ToE2() {
        ReceiveKNumBvalx(3, false, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateE2(false));
    }

    void ToE2(bool valx) {
        if (valx) {
            D4ToE1();
            E1ToE2();
        } else {
            D3ToE3();
            E3ToE2();
        }
    }

    void D3ToE3() {
        TransitToD3();
        EXPECT_TRUE(myba_state.CheckMyBAStateE3());
    }

    void E3ToE2() {
        ReceiveKNumBvalx(3, true, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateE2(true));
    }

    void E1ToF1() {
        Receive2fBval(true, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateF1());
    }

    void E1ToF2() {
        Receive2fBval(false, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateF2());
    }

    void E2ToF3() {
        Receive2fBval(true, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateF3());
    }

    void E3ToF4() {
        Receive2fBval(false, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateF4());
    }

    void toG1(bool val) {
        ReceiveKNumPromx(4, val, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateG1());
    }

    void toG2() {
        Receive2fPromAndAux(coin1_, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateG2());
    }

    void toG3() {
        Receive2fPromAndAux(!coin1_, round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateG3());
    }

    void toG4() {
        Receive2fPromAndAuxMixed(round1_);
        EXPECT_TRUE(myba_state.CheckMyBAStateG4());
    }

  private:
    Round round1_ = 1;
    bool coin1_ = false;
};

TEST_F(MyBAStateMachineTestRound1, TestE1ToE2) {
    D4ToE1();
    E1ToE2();
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToF1_byBval) {
    D4ToE1();

    Receive2fBval(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF1());
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToF1_byProm) {
    D4ToE1();

    ReceiveProm(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF1());
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToF1_byAux) {
    D4ToE1();

    Receive1Aux(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF1());
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToF2_byBval) {
    D4ToE1();

    Receive2fBval(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF2());
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToF2_byProm) {
    D4ToE1();

    ReceiveProm(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF2());
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToF2_byAux) {
    D4ToE1();

    Receive1Aux(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF2());
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToF2_byBval) {
    ToE2(GetParam());
    if (GetParam()) {
        ReceiveBvalx({4}, false, round1_);
    } else {
        Receive2fBval(false, round1_);
    }
    EXPECT_TRUE(myba_state.CheckMyBAStateF2());
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToF2_byProm) {
    ToE2(GetParam());

    ReceiveProm(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF2());
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToF2_byAux) {
    ToE2(GetParam());

    Receive1Aux(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF2());
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToF3_byBval) {
    ToE2(GetParam());
    if (GetParam()) {
        Receive2fBval(true, round1_);
    } else {
        ReceiveBvalx({4}, true, round1_);
    }

    EXPECT_TRUE(myba_state.CheckMyBAStateF3());
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToF3_byProm) {
    ToE2(GetParam());

    ReceiveProm(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF3());
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToF3_byAux) {
    ToE2(GetParam());

    Receive1Aux(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF3());
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToE2) {
    D3ToE3();

    ReceiveKNumBvalx(3, true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateE2(true));
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToF3_byBval) {
    D3ToE3();

    Receive2fBval(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF3());
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToF3_byProm) {
    D3ToE3();

    ReceiveProm(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF3());
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToF3_byAux) {
    D3ToE3();

    Receive1Aux(true, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF3());
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToF4_byBval) {
    D3ToE3();

    Receive2fBval(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF4());
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToF4_byProm) {
    D3ToE3();

    ReceiveProm(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF4());
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToF4_byAux) {
    D3ToE3();

    Receive1Aux(false, round1_);
    EXPECT_TRUE(myba_state.CheckMyBAStateF4());
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToG1) {
    D4ToE1();

    toG1(true);
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToG1) {
    ToE2(GetParam());

    toG1(false);
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToG1) {
    D3ToE3();

    toG1(false);
}

TEST_F(MyBAStateMachineTestRound1, TestF1ToG1) {
    D4ToE1();
    E1ToF1();

    toG1(true);
}

// TEST_P(MyBAStateMachineTestRound1, TestF2ToG1) {
//     D4ToE1();
//     Receive2fProm(false, round1_);

//     bool val = GetParam();
//     toG1(val);
// }

// TEST_P(MyBAStateMachineTestRound1, TestF3ToG1) {
//     D4ToE1();
//     E1ToE2();
//     Receive2fProm(true, round1_);

//     bool val = GetParam();
//     toG1(val);
// }

TEST_F(MyBAStateMachineTestRound1, TestF4ToG1) {
    D3ToE3();
    E3ToF4();

    toG1(false);
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToG2) {
    D4ToE1();
    toG2();
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToG2) {
    ToE2(GetParam());

    toG2();
}

TEST_F(MyBAStateMachineTestRound1, TestE3ToG2) {
    D3ToE3();

    toG2();
}

// TEST_F(MyBAStateMachineTestRound1, TestF1ToG2) {
//     D4ToE1();
//     E1ToF1();

//     toG2();
// }

TEST_F(MyBAStateMachineTestRound1, TestF2ToG2) {
    D4ToE1();
    E1ToF2();

    toG2();
}

// TEST_F(MyBAStateMachineTestRound1, TestF3ToG2) {
//     D4ToE1();
//     E1ToE2();
//     E2ToF3();

//     toG2();
// }

TEST_F(MyBAStateMachineTestRound1, TestF4ToG2) {
    D3ToE3();
    E3ToF4();

    toG2();
}

TEST_F(MyBAStateMachineTestRound1, TestE1ToG3) {
    D4ToE1();
    toG3();
}

TEST_P(MyBAStateMachineTestRound1, TestE2ToG3) {
    ToE2(GetParam());

    toG3();
}

TEST_P(MyBAStateMachineTestRound1, TestE3ToG3) {
    D3ToE3();

    toG2();
}

TEST_F(MyBAStateMachineTestRound1, TestF1ToG3) {
    D4ToE1();
    E1ToF1();

    toG3();
}

// TEST_F(MyBAStateMachineTestRound1, TestF2ToG3) {
//     D4ToE1();
//     E1ToF2();

//     toG3();
// }

TEST_F(MyBAStateMachineTestRound1, TestF3ToG3) {
    D4ToE1();
    E1ToE2();
    E2ToF3();

    toG3();
}

// TEST_F(MyBAStateMachineTestRound1, TestF4ToG3) {
//     D3ToE3();
//     E3ToF4();

//     toG3();
// }

INSTANTIATE_TEST_SUITE_P(BvalX, MyBAStateMachineTestRound1, testing::Values(false, true));

class MyBAStateMachineTestRound2 : public MyBAStateMachineTestRound1 {
  public:
    MyBAStateMachineTestRound2() : MyBAStateMachineTestRound1() {}

    void SetUp() override {
        coin2_ = myba_state.myba_no_aggrbval_->GetCoin(round2_);
    }

    void G3ToH1() {
        D4ToE1();
        toG3();
        EXPECT_TRUE(myba_state.CheckMyBAStateH1());
    }

    void H1ToH2() {
        ReceiveKNumBvalx(3, false, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateH2(false));
    }

    void ToH2(bool valx) {
        if (valx) {
            G3ToH1();
            H1ToH2();
        } else {
            G4ToH3();
            H3ToH2();
        }
    }

    void G4ToH3() {
        D4ToE1();
        toG4();
        EXPECT_TRUE(myba_state.CheckMyBAStateH3());
    }

    void H3ToH2() {
        ReceiveKNumBvalx(3, true, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateH2(true));
    }

    void H1ToI1() {
        Receive2fBval(true, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateI1());
    }

    void H1ToI2() {
        Receive2fBval(false, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateI2());
    }

    void H2ToI3() {
        Receive2fBval(true, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateI3());
    }

    void H3ToI4() {
        Receive2fBval(false, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateI4());
    }

    void toJ1(bool val) {
        ReceiveKNumPromx(4, val, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateJ1());
    }

    void toJ2() {
        Receive2fPromAndAux(coin2_, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateJ2());
    }

    void toJ3() {
        Receive2fPromAndAux(!coin2_, round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateJ3());
    }

    void toJ4() {
        Receive2fPromAndAuxMixed(round2_);
        EXPECT_TRUE(myba_state.CheckMyBAStateJ4());
    }

  private:
    Round round2_ = 2;
    bool coin2_ = true;
};

TEST_F(MyBAStateMachineTestRound2, TestH1ToH2) {
    G3ToH1();
    H1ToH2();
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToI1_byBval) {
    G3ToH1();

    Receive2fBval(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI1());
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToI1_byProm) {
    G3ToH1();

    ReceiveProm(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI1());
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToI1_byAux) {
    G3ToH1();

    Receive1Aux(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI1());
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToI2_byBval) {
    G3ToH1();

    Receive2fBval(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI2());
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToI2_byProm) {
    G3ToH1();

    ReceiveProm(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI2());
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToI2_byAux) {
    G3ToH1();

    Receive1Aux(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI2());
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToI2_byBval) {
    ToH2(GetParam());

    if (GetParam()) {
        ReceiveBvalx({4}, false, round2_);
    } else {
        Receive2fBval(false, round2_);
    }

    EXPECT_TRUE(myba_state.CheckMyBAStateI2());
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToI2_byProm) {
    ToH2(GetParam());

    ReceiveProm(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI2());
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToI2_byAux) {
    ToH2(GetParam());

    Receive1Aux(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI2());
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToI3_byBval) {
    ToH2(GetParam());

    if (GetParam()) {
        Receive2fBval(true, round2_);
    } else {
        ReceiveBvalx({4}, true, round2_);
    }

    EXPECT_TRUE(myba_state.CheckMyBAStateI3());
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToI3_byProm) {
    ToH2(GetParam());

    ReceiveProm(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI3());
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToI3_byAux) {
    ToH2(GetParam());

    Receive1Aux(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI3());
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToH2) {
    G4ToH3();
    ReceiveKNumBvalx(3, true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateH2(true));
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToI3_byBval) {
    G4ToH3();

    Receive2fBval(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI3());
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToI3_byProm) {
    G4ToH3();

    ReceiveProm(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI3());
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToI3_byAux) {
    G4ToH3();

    Receive1Aux(true, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI3());
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToI4_byBval) {
    G4ToH3();

    Receive2fBval(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI4());
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToI4_byProm) {
    G4ToH3();

    ReceiveProm(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI4());
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToI4_byAux) {
    G4ToH3();

    Receive1Aux(false, round2_);
    EXPECT_TRUE(myba_state.CheckMyBAStateI4());
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToJ1) {
    G3ToH1();

    toJ1(true);
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToJ1) {
    ToH2(GetParam());

    toJ1(true);
}

TEST_P(MyBAStateMachineTestRound2, TestH3ToJ1) {
    G4ToH3();

    bool val = GetParam();
    toJ1(val);
}

TEST_F(MyBAStateMachineTestRound2, TestI1ToJ1) {
    G3ToH1();
    H1ToI1();

    toJ1(true);
}

// TEST_P(MyBAStateMachineTestRound2, TestI2ToJ1) {
//     G3ToH1();
//     H1ToI2();

//     bool val = GetParam();
//     toJ1(val);
// }

// TEST_P(MyBAStateMachineTestRound2, TestI3ToJ1) {
//     G3ToH1();
//     H1ToH2();
//     H2ToI3();

//     bool val = GetParam();
//     toJ1(val);
// }

TEST_F(MyBAStateMachineTestRound2, TestI4ToJ1) {
    G4ToH3();
    H3ToI4();

    toJ1(false);
}

TEST_F(MyBAStateMachineTestRound2, TestH1ToJ2) {
    G3ToH1();

    toJ2();
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToJ2) {
    ToH2(GetParam());

    toJ2();
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToJ2) {
    G4ToH3();

    toJ2();
}

TEST_F(MyBAStateMachineTestRound2, TestI1ToJ2) {
    G3ToH1();
    H1ToI1();

    toJ2();
}

// TEST_F(MyBAStateMachineTestRound2, TestI2ToJ2) {
//     G3ToH1();
//     H1ToI2();

//     toJ2();
// }

TEST_F(MyBAStateMachineTestRound2, TestI3ToJ2) {
    G3ToH1();
    H1ToH2();
    H2ToI3();

    toJ2();
}

// TEST_F(MyBAStateMachineTestRound2, TestI4ToJ2) {
//     G4ToH3();
//     H3ToI4();

//     toJ2();
// }

TEST_F(MyBAStateMachineTestRound2, TestH1ToJ3) {
    G3ToH1();
    toJ3();
}

TEST_P(MyBAStateMachineTestRound2, TestH2ToJ3) {
    ToH2(GetParam());

    toJ3();
}

TEST_F(MyBAStateMachineTestRound2, TestH3ToJ3) {
    G4ToH3();

    toJ3();
}

// TEST_F(MyBAStateMachineTestRound2, TestI1ToJ3) {
//     G3ToH1();
//     H1ToI1();

//     toJ3();
// }

TEST_F(MyBAStateMachineTestRound2, TestI2ToJ3) {
    G3ToH1();
    H1ToI2();

    toJ3();
}

// TEST_F(MyBAStateMachineTestRound2, TestI3ToJ3) {
//     G3ToH1();
//     H1ToH2();
//     H2ToI3();

//     toJ3();
// }

TEST_F(MyBAStateMachineTestRound2, TestF4ToJ3) {
    G4ToH3();
    H3ToI4();

    toJ3();
}

INSTANTIATE_TEST_SUITE_P(BvalX, MyBAStateMachineTestRound2, testing::Values(false, true));

}  // namespace consensus_spec
