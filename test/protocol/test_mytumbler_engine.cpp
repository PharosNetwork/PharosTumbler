// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "mytumbler_engine_fixture.h"

namespace consensus_spec {

namespace {
void InitDummyCryptoFunc(std::shared_ptr<MyTumblerEngineBase>& engine) {
    auto digest = [](const std::string&) { return std::string("digest"); };
    auto sign = [](const std::string&, std::string& sig) { sig = "sig"; };
    auto verify = [](const std::string&, const std::string&,
                    const std::string&) { return true; };
    auto ecc_signer = std::make_shared<ECCSigner>(sign, verify);
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
    auto aggregator = std::make_shared<BLSAggregator>(
        aggr_sign, aggr_verify, aggr_signature, opt_aggr_verify, gen_bitmap, extract_nodes);
    engine->InitCryptoFunc(digest, ecc_signer, aggregator);
}
}  // namespace

#define NOWTIME                                                                                  \
    static_cast<long long unsigned int>(std::chrono::duration_cast<std::chrono::milliseconds>(   \
                                            std::chrono::system_clock::now().time_since_epoch()) \
                                            .count())

class MyTumblerEngineTest : public testing::Test, public mytumbler_engine_fixture {
  public:
    void InsertLog(MyTumblerEngineBasePtr engine_,
                   std::vector<std::pair<NodeId, MessagePtr>> msgs) {
        for (auto& msg : msgs) {
            std::dynamic_pointer_cast<ReliableChannel>(engine_->reliable_channel_)
                ->DoSaveMessage(msg.second);
            std::dynamic_pointer_cast<ReliableChannel>(engine_->reliable_channel_)->Flush();
        }
    }

    void DoConfigureTest() {
        MyTumblerConfig cc = cc_;
        InitDummyCryptoFunc(engine_);

        // V7+ requires spec_version >= CONSENSUS_VERSION_MIN and non-empty proposers_map
        std::map<Seq, std::set<NodeId>> proposers_map;
        proposers_map[0] = std::set<NodeId>(peers_.begin(), peers_.end());

        // Normal case: should succeed (version 7, non-empty proposers_map)
        EXPECT_TRUE(engine_->Configure(CONSENSUS_VERSION_PIPELINE_PROPOSE,
                                       7,
                                       my_id_,
                                       peer_pubkey_map_,
                                       peer_balance_map_,
                                       cc,
                                       0,
                                       0,
                                       0,
                                       false,
                                       proposers_map,
                                       CreateWALStorage("log/configure_test"),
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       nullptr));
        EXPECT_EQ(engine_->myba_.size(), 4);

        // Corner case: version < CONSENSUS_VERSION_MIN should return false
        engine_ = std::make_shared<MyTumblerEngineBase>();
        InitDummyCryptoFunc(engine_);
        EXPECT_FALSE(engine_->Configure(CONSENSUS_VERSION_MIN - 1,
                                        7,
                                        my_id_,
                                        peer_pubkey_map_,
                                        peer_balance_map_,
                                        cc,
                                        0,
                                        0,
                                        0,
                                        false,
                                        proposers_map,
                                        CreateWALStorage("log/configure_test"),
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr));

        // Corner case 1: V7+ with empty proposers_map
        // Configure should return false because itr == proposers_map.begin()
        engine_ = std::make_shared<MyTumblerEngineBase>();
        InitDummyCryptoFunc(engine_);
        std::map<Seq, std::set<NodeId>> empty_proposers_map;

        EXPECT_FALSE(engine_->Configure(CONSENSUS_VERSION_PIPELINE_PROPOSE,
                                        7,
                                        my_id_,
                                        peer_pubkey_map_,
                                        peer_balance_map_,
                                        cc,
                                        0,
                                        0,
                                        0,
                                        false,
                                        empty_proposers_map,
                                        CreateWALStorage("log/configure_empty_proposers_test"),
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr));

        // Corner case 2: ConfigureDB with nullptr db
        // If need_persist_message_ is true but db is nullptr, Configure should return false
        engine_ = std::make_shared<MyTumblerEngineBase>();
        MyTumblerConfig cc_with_persist = cc_;
        cc_with_persist.need_persist_message_ = true;
        InitDummyCryptoFunc(engine_);

        EXPECT_FALSE(engine_->Configure(CONSENSUS_VERSION_PIPELINE_PROPOSE,
                                        7,
                                        my_id_,
                                        peer_pubkey_map_,
                                        peer_balance_map_,
                                        cc_with_persist,
                                        0,
                                        0,
                                        0,
                                        false,
                                        proposers_map,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr,
                                        nullptr));
    }

    void DoConfigureWithMessagesInDBTest1() {
        Seq seq = 1;
        bytes payload = asBytes("payload");
        MyTumblerConfig cc = cc_;

        std::map<Seq, std::set<NodeId>> proposers_map;
        proposers_map[0] = std::set<NodeId>(peers_.begin(), peers_.end());

        // empty db
        std::shared_ptr<wal::WalLocalStorage> local_db = CreateWALStorage("log/db_test");
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
        {
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
            engine_->InitCryptoFunc(
                [this](const std::string& data) {
                    return "digest";
                },
                ecc_signer,
                dummy_aggregator);
        }
        EXPECT_TRUE(engine_->Configure(
            CONSENSUS_VERSION_PIPELINE_PROPOSE,
            7,
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc,
            0,
            0,
            0,
            false,
            proposers_map,
            local_db,
            [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
                return;
            },
            [this](const consensus_spec::MessagePtr msg) {
                return;
            },
            nullptr,
            [this](Seq seq,
                   uint64_t ts,
                   Digest digest,
                   std::vector<ssz_types::VoteStatus>& vote_status) {
                return std::make_pair(
                    false,
                    std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
            },
            [](std::vector<BlockProofBuffer> stable_proofs,
               std::vector<BlockProofBuffer> checkpoints) {
                return;
            }));
        EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

        // load my val msg (epoch must match current epoch for V7+ epoch filtering)
        ValMessagePtr v0 = CreateValMessage(seq, peers_[0], NOWTIME, payload, engine_->epoch_number_);
        std::vector<std::pair<NodeId, MessagePtr>> persistent_msgs = {
            {peers_[0], std::make_shared<ssz_types::ConsensusMessage>(v0)}};
        MyTumblerEngineTest::InsertLog(engine_, persistent_msgs);
        local_db = std::make_shared<wal::WalLocalStorage>(local_db->option_);
        ecc_signer = std::make_shared<ECCSigner>(
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
        {
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
            engine_->InitCryptoFunc(
                [this](const std::string& data) {
                    return "digest";
                },
                ecc_signer,
                dummy_aggregator);
        }
        EXPECT_TRUE(engine_->Configure(
            CONSENSUS_VERSION_PIPELINE_PROPOSE,
            7,
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc,
            0,
            0,
            0,
            false,
            proposers_map,
            local_db,
            [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
                return;
            },
            [this](const consensus_spec::MessagePtr msg) {
                return;
            },
            nullptr,
            [this](Seq seq,
                   uint64_t ts,
                   Digest digest,
                   std::vector<ssz_types::VoteStatus>& vote_status) {
                return std::make_pair(
                    false,
                    std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
            },
            [](std::vector<BlockProofBuffer> stable_proofs,
               std::vector<BlockProofBuffer> checkpoints) {
                return;
            }));
        EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());
        EXPECT_EQ(engine_->endorsed_[seq].size(), 1);

        // load my val msg and three skip msgs (skips from others only: no my_id_ in skipped_
        // so load path does not advance propose_seq_ to 2)
        engine_->ClearCurrSeqState(seq);
        persistent_msgs.clear();
        engine_->propose_state_.is_proposed_.store(false);
        SkipMessagePtr s1 = CreateSkipMessage(seq, peers_[1], Signature());
        SkipMessagePtr s2 = CreateSkipMessage(seq, peers_[2], Signature());
        SkipMessagePtr s3 = CreateSkipMessage(seq, peers_[3], Signature());
        persistent_msgs = {{peers_[1], std::make_shared<ssz_types::ConsensusMessage>(s1)},
                           {peers_[2], std::make_shared<ssz_types::ConsensusMessage>(s2)},
                           {peers_[3], std::make_shared<ssz_types::ConsensusMessage>(s3)}};
        MyTumblerEngineTest::InsertLog(engine_, persistent_msgs);
        local_db = std::make_shared<wal::WalLocalStorage>(local_db->option_);
        EXPECT_EQ(engine_->skipped_.size(), 0);
        EXPECT_EQ(engine_->passed_.size(), 0);
        EXPECT_EQ(engine_->endorsed_.size(), 0);
        ecc_signer = std::make_shared<ECCSigner>(
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
        {
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
            engine_->InitCryptoFunc(
                [this](const std::string& data) {
                    return "digest";
                },
                ecc_signer,
                dummy_aggregator);
        }
        EXPECT_TRUE(engine_->Configure(
            CONSENSUS_VERSION_PIPELINE_PROPOSE,
            7,
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc,
            0,
            0,
            0,
            false,
            proposers_map,
            local_db,
            [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
                return;
            },
            [this](const consensus_spec::MessagePtr msg) {
                return;
            },
            [](Seq, const NodeId&, const std::pair<uint32_t, uint32_t>, std::shared_ptr<ABuffer>) {
                return true;
            },
            [this](Seq seq,
                   uint64_t ts,
                   Digest digest,
                   std::vector<ssz_types::VoteStatus>& vote_status) {
                return std::make_pair(
                    false,
                    std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
            },
            [](std::vector<BlockProofBuffer> stable_proofs,
               std::vector<BlockProofBuffer> checkpoints) {
                return;
            }));
        engine_->Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        engine_->Stop();
        EXPECT_EQ(engine_->skipped_.size(), 3);
        // V7+: skips from others only, so propose_seq_ not advanced in load path; no Pass in DB so passed_ empty
        EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), seq);
        EXPECT_EQ(engine_->endorsed_[seq].size(), 1);
    }

    void DoConfigureWithMessagesInDBTest2() {
        Seq seq = 1;
        bytes payload = asBytes("payload");
        MyTumblerConfig cc = cc_;

        std::map<Seq, std::set<NodeId>> proposers_map;
        proposers_map[0] = std::set<NodeId>(peers_.begin(), peers_.end());

        std::shared_ptr<wal::WalLocalStorage> local_db = CreateWALStorage("log/db_test");
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
        {
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
            engine_->InitCryptoFunc(
                [this](const std::string& data) {
                    return "digest";
                },
                ecc_signer,
                dummy_aggregator);
        }
        EXPECT_TRUE(engine_->Configure(
            CONSENSUS_VERSION_PIPELINE_PROPOSE,
            7,
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc,
            0,
            0,
            0,
            false,
            proposers_map,
            local_db,
            [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
                return;
            },
            [this](const consensus_spec::MessagePtr msg) {
                return;
            },
            nullptr,
            [this](Seq seq,
                   uint64_t ts,
                   Digest digest,
                   std::vector<ssz_types::VoteStatus>& vote_status) {
                return std::make_pair(
                    false,
                    std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
            },
            [](std::vector<BlockProofBuffer> stable_proofs,
               std::vector<BlockProofBuffer> checkpoints) {
                return;
            }));
        EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

        // load my skip msg, then reconfigure with V7+ and verify state
        engine_->ClearCurrSeqState(seq);
        engine_->propose_state_.is_proposed_.store(false);
        SkipMessagePtr s0 = CreateSkipMessage(seq, peers_[0], Signature());
        std::vector<std::pair<NodeId, MessagePtr>> persistent_msgs = {
            {peers_[0], std::make_shared<ssz_types::ConsensusMessage>(s0)}};
        MyTumblerEngineTest::InsertLog(engine_, persistent_msgs);
        local_db = std::make_shared<wal::WalLocalStorage>(local_db->option_);
        EXPECT_EQ(engine_->skipped_.size(), 0);
        ecc_signer = std::make_shared<ECCSigner>(
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
        {
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
            engine_->InitCryptoFunc(
                [this](const std::string& data) {
                    return "digest";
                },
                ecc_signer,
                dummy_aggregator);
        }
        EXPECT_TRUE(engine_->Configure(
            CONSENSUS_VERSION_PIPELINE_PROPOSE,
            0,  // epoch
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc,
            0,
            0,
            0,
            false,
            proposers_map,
            local_db,
            [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
                return;
            },
            [this](const consensus_spec::MessagePtr msg) {
                return;
            },
            nullptr,
            [this](Seq seq,
                   uint64_t ts,
                   Digest digest,
                   std::vector<ssz_types::VoteStatus>& vote_status) {
                return std::make_pair(
                    false,
                    std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
            },
            [](std::vector<BlockProofBuffer> stable_proofs,
               std::vector<BlockProofBuffer> checkpoints) {
                return;
            }));
        EXPECT_EQ(engine_->skipped_.size(), 1);
        EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
        EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);

        // load pass msg contains one endorse and two val msgs
        engine_->ClearCurrSeqState(seq);
        persistent_msgs.clear();
        engine_->propose_state_.is_proposed_.store(false);
        std::vector<ssz::ByteVector<32>> tmp_endorsed;
        tmp_endorsed.emplace_back(peers_[1]);
        PassMessagePtr p0 = CreatePassMessage(seq, std::move(tmp_endorsed));
        ValMessagePtr v2 = CreateValMessage(seq, peers_[2], NOWTIME, payload);
        ValMessagePtr v3 = CreateValMessage(seq, peers_[3], NOWTIME, payload);
        persistent_msgs = {{peers_[0], std::make_shared<ssz_types::ConsensusMessage>(p0)},
                           {peers_[2], std::make_shared<ssz_types::ConsensusMessage>(v2)},
                           {peers_[3], std::make_shared<ssz_types::ConsensusMessage>(v3)}};
        MyTumblerEngineTest::InsertLog(engine_, persistent_msgs);
        local_db = std::make_shared<wal::WalLocalStorage>(local_db->option_);
        EXPECT_EQ(engine_->passed_.size(), 0);
        EXPECT_EQ(engine_->endorsed_[seq].size(), 0);
        ecc_signer = std::make_shared<ECCSigner>(
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
        {
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
            engine_->InitCryptoFunc(
                [this](const std::string& data) {
                    return "digest";
                },
                ecc_signer,
                dummy_aggregator);
        }
        EXPECT_TRUE(engine_->Configure(
            CONSENSUS_VERSION_PIPELINE_PROPOSE,
            7,
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc,
            0,
            0,
            0,
            false,
            proposers_map,
            local_db,
            [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
                return;
            },
            [this](const consensus_spec::MessagePtr msg) {
                return;
            },
            [](Seq, const NodeId&, const std::pair<uint32_t, uint32_t>, std::shared_ptr<ABuffer>) {
                return true;
            },
            [this](Seq seq,
                   uint64_t ts,
                   Digest digest,
                   std::vector<ssz_types::VoteStatus>& vote_status) {
                return std::make_pair(
                    false,
                    std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
            },
            [](std::vector<BlockProofBuffer> stable_proofs,
               std::vector<BlockProofBuffer> checkpoints) {
                return;
            }));
        engine_->Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        engine_->Stop();
        EXPECT_EQ(engine_->passed_.size(), 1);
        EXPECT_EQ(engine_->endorsed_[seq].size(), 1);
    }

    void DoAsyncProposeTest() {
        bytes batch;

        engine_->propose_state_.AdvanceProposeState(true, 1);
        engine_->Start();
        engine_->Propose(batch, false, 1111, nullptr);
        std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
        engine_->Propose(batch, false, 1111, &prom);
        auto future = prom.get_future();
        EXPECT_EQ(future.get(),
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS,
                      0)));

        // pipeline window full
        prom = std::promise<std::pair<MyTumblerProposeState, Seq>>();
        engine_->propose_state_.AdvanceProposeState(false, 11);
        engine_->last_consensus_seq_.store(10);
        engine_->Propose(batch, false, 1111, &prom);
        future = prom.get_future();
        EXPECT_EQ(future.get(),
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL,
                      0)));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        // has passed
        prom = std::promise<std::pair<MyTumblerProposeState, Seq>>();
        engine_->last_consensus_seq_.store(0);
        engine_->propose_state_.is_proposed_.store(false);
        engine_->propose_state_.propose_seq_.store(1);
        engine_->passed_[peers_[0]] = CreatePassMessage(1, std::vector<ssz::ByteVector<32>>());
        engine_->Propose(batch, false, 1111, &prom);
        future = prom.get_future();
        EXPECT_EQ(future.get(),
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS,
                      0)));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        engine_->passed_.clear();

        // // has skipped
        // prom = std::promise<std::pair<MyTumblerProposeState, Seq>>();
        // engine_->passed_.clear();
        // engine_->skipped_.emplace(peers_[0], "signature");
        // engine_->Propose(batch, false, 1111, &prom);
        // future = prom.get_future();
        // EXPECT_EQ(future.get(), MyTumblerProposeState::PROPOSE_FAILED_REFUSE);
        // std::this_thread::sleep_for(std::chrono::milliseconds(20));
        // engine_->skipped_.clear();

        // check validity
        auto check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
            return false;
        };
        engine_->check_validity_ = check_validity;
        prom = std::promise<std::pair<MyTumblerProposeState, Seq>>();
        engine_->Propose(batch, false, 1111, &prom);
        future = prom.get_future();
        EXPECT_EQ(
            future.get(),
            (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS, 1)));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        engine_->Stop();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    void DoAsyncProposeNormalTest() {
        // OK
        std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
        engine_->Start();
        bytes batch;
        engine_->Propose(batch, false, 1111, &prom);
        auto future = prom.get_future();
        EXPECT_EQ(
            future.get(),
            (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS, 1)));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        engine_->Stop();
    }

    void DoAsyncEmptyProposeTest() {
        engine_->Start();
        bytes batch;
        std::promise<std::pair<MyTumblerProposeState, Seq>> prom;

        // my_index = 1, not leader
        engine_->propose_empty_turn_ = 1;
        engine_->Propose(batch, true, 1111, &prom);
        auto future = prom.get_future();
        EXPECT_EQ(future.get(),
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_NOT_ALLOW_EMPTY,
                      0)));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));

        // my_index = 1, leader
        prom = std::promise<std::pair<MyTumblerProposeState, Seq>>();
        engine_->propose_empty_turn_ = 4;
        engine_->last_consensus_seq_.store(3);
        engine_->propose_state_.AdvanceProposeState(false, 4);
        engine_->Propose(batch, true, 1111, &prom);
        future = prom.get_future();
        EXPECT_EQ(
            future.get(),
            (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS, 4)));

        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        engine_->Stop();
    }

    void DoEndorseCallBackTest() {
        engine_->DoEndorseCallBack(0, peers_[1], Digest("1"));
        EXPECT_TRUE(engine_->already_vote_zero_.empty());
        EXPECT_TRUE(engine_->endorsed_.empty());

        engine_->DoEndorseCallBack(1, peers_[1], Digest("1"));
        EXPECT_TRUE(engine_->already_vote_zero_.empty());
        EXPECT_EQ(engine_->endorsed_[1].size(), 1);

        engine_->passed_[peers_[0]] = CreatePassMessage(1, std::vector<ssz::ByteVector<32>>());
        engine_->DoEndorseCallBack(1, peers_[1], Digest("1"));
        EXPECT_EQ(engine_->already_vote_zero_.size(), 1);
    }

    void DoMyBACompleteCallBackTest() {
        // outdated
        auto new_check_validity = [&](Seq seq,
                                      const NodeId& id,
                                      const std::pair<uint32_t, uint32_t> index_epoch,
                                      std::shared_ptr<ABuffer> buffer) {
            engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
            return true;
        };
        engine_->check_validity_ = new_check_validity;
        engine_->DoMyBACompleteCallBack(0, peers_[0], Digest("1"));
        EXPECT_TRUE(engine_->ba_complete_.empty());
        EXPECT_TRUE(engine_->ba_success_.empty());

        bytes payload = asBytes("payload");
        auto v1 = CreateValMessage(1, peers_[0], NOWTIME, payload);
        Signature tmp_sig;
        engine_->crypto_helper_->Sign(CalculateBvalDigestForValMessage(v1, engine_->spec_version_),
                                      tmp_sig);
        v1->signature = tmp_sig;
        auto v2 = CreateValMessage(1, peers_[2], NOWTIME, payload);
        engine_->crypto_helper_->Sign(CalculateBvalDigestForValMessage(v2, engine_->spec_version_),
                                      tmp_sig);
        v2->signature = tmp_sig;
        auto v3 = CreateValMessage(1, peers_[3], NOWTIME, payload);
        engine_->crypto_helper_->Sign(CalculateBvalDigestForValMessage(v3, engine_->spec_version_),
                                      tmp_sig);
        v3->signature = tmp_sig;
        Seq cur_seq = engine_->GetCurrSeq();
        engine_->pending_[cur_seq].insert(peers_[0]);
        engine_->pending_[cur_seq].insert(peers_[1]);
        engine_->pending_[cur_seq].insert(peers_[2]);
        engine_->pending_[cur_seq].insert(peers_[3]);
        EXPECT_EQ(engine_->pending_[cur_seq].size(), 4);

        engine_->DoMyBACompleteCallBack(1, peers_[1], ZERO_32_BYTES);
        EXPECT_TRUE(engine_->ba_success_.empty());
        EXPECT_TRUE(engine_->output_.empty());
        EXPECT_EQ(engine_->ba_complete_.size(), 1);
        // if self finished 0 without proposal, then set in_consensus to true
        // EXPECT_TRUE(engine_->in_consensus_);

        EXPECT_EQ(engine_->pending_[cur_seq].size(), 3);
        EXPECT_TRUE(!engine_->pending_[cur_seq].count(peers_[1]));

        engine_->DoMyBACompleteCallBack(1, peers_[0], Digest(v1->hash.Acquire()));
        EXPECT_EQ(engine_->ba_success_.size(), 1);
        EXPECT_EQ(engine_->output_.size(), 1);
        EXPECT_EQ(engine_->ba_complete_.size(), 2);
        EXPECT_EQ(engine_->pending_[cur_seq].size(), 3);

        engine_->DoMyBACompleteCallBack(1, peers_[2], Digest(v2->hash.Acquire()));
        EXPECT_EQ(engine_->ba_success_.size(), 2);
        EXPECT_EQ(engine_->output_.size(), 2);
        EXPECT_EQ(engine_->ba_complete_.size(), 3);
        EXPECT_EQ(engine_->pending_[cur_seq].size(), 3);

        engine_->DoMyBACompleteCallBack(1, peers_[3], Digest(v3->hash.Acquire()));
        EXPECT_EQ(engine_->ba_success_.size(), 3);
        EXPECT_EQ(engine_->output_.size(), 3);
        EXPECT_EQ(engine_->ba_complete_.size(), 4);
        EXPECT_EQ(engine_->pending_[cur_seq].size(), 3);
        EXPECT_TRUE(!engine_->passed_.empty());

        engine_->DoOnRecvValMessage(peers_[0], v1, RECEIVED_FROM_OTHERS);
        engine_->DoOnRecvValMessage(peers_[2], v2, RECEIVED_FROM_OTHERS);
        engine_->DoOnRecvValMessage(peers_[3], v3, RECEIVED_FROM_OTHERS);
        EXPECT_TRUE(engine_->pending_[cur_seq].empty());
    }

    void DoOnRecvValMessageTest() {
        engine_->DoOnRecvValMessage(peers_[1], nullptr, RECEIVED_FROM_OTHERS);
        EXPECT_TRUE(engine_->val_.empty());

        bytes payload = asBytes("payload");

        // invalid timestamp
        engine_->DoUpdateRawSeq(1, 100, false, false);
        auto val = CreateValMessage(2, peers_[1], 99, payload);
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_TRUE(engine_->val_.empty());

        // invalid hash
        engine_->DoUpdateRawSeq(1, 100, false, false);
        val = CreateValMessage(2, peers_[1], 101, payload);
        val->hash = ZERO_32_BYTES;
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_TRUE(engine_->val_.empty());

        engine_->DoUpdateRawSeq(1, 100, false, false);
        engine_->last_consensus_seq_.store(1);
        val = CreateValMessage(2, peers_[1], 101, payload);
        Signature tmp_sig;
        engine_->crypto_helper_->Sign(CalculateBvalDigestForValMessage(val, engine_->spec_version_),
                                      tmp_sig);
        val->signature = tmp_sig;

        // check validity
        auto check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
            return false;
        };
        engine_->check_validity_ = check_validity;
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_TRUE(engine_->val_.empty());

        // normal test
        int check_sanity_counter = 0;
        auto new_check_validity = [&](Seq seq,
                                      const NodeId& id,
                                      const std::pair<uint32_t, uint32_t> index_epoch,
                                      std::shared_ptr<ABuffer> buffer) {
            ++check_sanity_counter;
            return true;
        };
        engine_->check_validity_ = new_check_validity;
        engine_->unchecked_proposals_.clear();
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        Seq cur_seq = engine_->GetCurrSeq();
        EXPECT_EQ(check_sanity_counter, 1);
        EXPECT_TRUE(engine_->val_.empty());
        EXPECT_TRUE(engine_->endorsed_.empty());
        EXPECT_TRUE(engine_->pending_.count(cur_seq) == 0);

        // receive again
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_EQ(check_sanity_counter, 1);

        engine_->DoOnCheckSanityDone(val->seq,
                                     peers_[1],
                                     std::make_pair(1, engine_->epoch_number_),
                                     std::nullopt);
        EXPECT_TRUE(!engine_->val_.empty());
        EXPECT_TRUE(!engine_->endorsed_.empty());
        EXPECT_TRUE(!engine_->endorsed_[cur_seq].empty());
        EXPECT_TRUE(!engine_->pending_[cur_seq].empty());

        // receive again
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_EQ(check_sanity_counter, 1);

        engine_->DoMyBACompleteCallBack(2, peers_[1], Digest(val->hash.Acquire()));
        EXPECT_TRUE(engine_->pending_[cur_seq].empty());
        EXPECT_TRUE(!engine_->ba_complete_.empty());

        // receive again after complete
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_EQ(check_sanity_counter, 1);
        EXPECT_TRUE(engine_->pending_[cur_seq].empty());

        // receive another but val_ has empty slot
        auto val2 = CreateValMessage(2, peers_[2], 101, payload);
        engine_->val_[val2->seq][peers_[2]];
        engine_->DoOnRecvValMessage(peers_[2], val2, RECEIVED_FROM_OTHERS);
        EXPECT_EQ(check_sanity_counter, 2);
        EXPECT_TRUE(engine_->pending_[cur_seq].empty());

        // old timestamp but v9
        engine_->spec_version_ = CONSENSUS_VERSION_NO_TIMESTAMP_CHECK;
        engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
        auto val3 = CreateValMessage(2, peers_[3], 1, payload);
        engine_->DoOnRecvValMessage(peers_[3], val3, RECEIVED_FROM_OTHERS);
        EXPECT_EQ(check_sanity_counter, 3);

        // has passed
        engine_->val_.clear();
        engine_->unchecked_proposals_.clear();
        engine_->ClearCurrSeqState(2);
        engine_->passed_[peers_[0]] = CreatePassMessage(2, std::vector<ssz::ByteVector<32>>());
        engine_->DoOnRecvValMessage(peers_[1], val, RECEIVED_FROM_OTHERS);
        EXPECT_EQ(check_sanity_counter, 4);
        EXPECT_TRUE(engine_->val_.empty());
        EXPECT_TRUE(engine_->endorsed_.empty());
        EXPECT_TRUE(engine_->already_vote_zero_.empty());
        engine_->DoOnCheckSanityDone(val->seq,
                                     peers_[1],
                                     std::make_pair(1, engine_->epoch_number_),
                                     std::nullopt);
        EXPECT_TRUE(!engine_->val_.empty());
        EXPECT_TRUE(engine_->endorsed_.empty());
        EXPECT_TRUE(!engine_->already_vote_zero_.empty());
    }

    void DoOnRecvSkipMessageTest() {
        engine_->DoOnRecvSkipMessage(peers_[1], nullptr);
        EXPECT_TRUE(engine_->skipped_.empty());

        auto skip = CreateSkipMessage(1, peers_[1], Signature());
        engine_->DoOnRecvSkipMessage(peers_[1], skip);
        EXPECT_TRUE(engine_->skipped_.empty());

        // engine_->crypto_helper_->Sign(CalculateSkipDigest(skip), tmp_sig);
        skip->signature = MockSignFunc(1, CalculateSkipDigest(skip));
        engine_->DoOnRecvSkipMessage(peers_[1], skip);
        EXPECT_EQ(engine_->skipped_.size(), 1);
        EXPECT_TRUE(engine_->skipped_.count(peers_[1]));

        auto s2 = CreateSkipMessage(1, peers_[2], Signature());
        // engine_->crypto_helper_->Sign(CalculateSkipDigest(s2), tmp_sig);
        s2->signature = MockSignFunc(2, CalculateSkipDigest(s2));
        engine_->DoOnRecvSkipMessage(peers_[2], s2);
        auto s3 = CreateSkipMessage(1, peers_[0], Signature());
        Signature tmp_sig;
        engine_->crypto_helper_->Sign(CalculateSkipDigest(s3), tmp_sig);
        s3->signature = tmp_sig;
        engine_->DoOnRecvSkipMessage(peers_[0], s3);
        auto s4 = CreateSkipMessage(1, peers_[3], Signature());
        s4->signature = MockSignFunc(3, CalculateSkipDigest(s3));
        engine_->DoOnRecvSkipMessage(peers_[3], s4);
        EXPECT_TRUE(!engine_->passed_.empty());

        engine_->output_.clear();
        engine_->DoOnRecvSkipMessage(peers_[1], skip);
        EXPECT_TRUE(engine_->output_.empty());
    }

    void DoOnRecvForwardSkipMessageTest() {
        engine_->DoOnRecvForwardSkipMessage(peers_[1], nullptr);
        EXPECT_TRUE(engine_->skipped_.empty());

        std::vector<ssz_types::Signature> signatures;
        auto forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
        engine_->DoOnRecvForwardSkipMessage(peers_[1], forward_skip);
        EXPECT_TRUE(engine_->skipped_.empty());

        signatures = std::vector<ssz_types::Signature>();
        auto skip = CreateSkipMessage(1, peers_[1], Signature());
        ssz_types::Signature signature1;
        signature1.node_id = peers_[1];
        signature1.signature = MockSignFunc(1, CalculateSkipDigest(skip));
        signatures.push_back(std::move(signature1));
        forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
        engine_->DoOnRecvForwardSkipMessage(peers_[1], forward_skip);
        EXPECT_EQ(engine_->skipped_.size(), 1);
        EXPECT_EQ(engine_->output_.size(), 1);
        EXPECT_EQ(engine_->output_balance_, (*peer_balance_map_)[peers_[1]]);
        EXPECT_TRUE(engine_->skipped_.count(peers_[1]));

        signatures = std::vector<ssz_types::Signature>();
        auto s1 = CreateSkipMessage(1, peers_[1], Signature());
        signature1.node_id = peers_[1];
        signature1.signature = MockSignFunc(1, CalculateSkipDigest(s1));
        signatures.push_back(std::move(signature1));
        auto s2 = CreateSkipMessage(1, peers_[2], Signature());
        ssz_types::Signature signature2;
        signature2.node_id = peers_[2];
        signature2.signature = MockSignFunc(2, CalculateSkipDigest(s2));
        ;
        signatures.push_back(std::move(signature2));
        auto s3 = CreateSkipMessage(1, peers_[0], Signature());
        ssz_types::Signature signature3;
        signature3.node_id = peers_[0];
        signature3.signature = MockSignFunc(0, CalculateSkipDigest(s3));
        ;
        signatures.push_back(std::move(signature3));
        forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
        engine_->DoOnRecvForwardSkipMessage(peers_[3], forward_skip);
        EXPECT_EQ(engine_->skipped_.size(), 3);
        EXPECT_EQ(engine_->output_.size(), 3);
        EXPECT_EQ(engine_->output_balance_, 9);
        // V7+ (limit proposer): Pass requires all proposers in output; 0+1+2 only -> not enough, must not Pass
        EXPECT_TRUE(engine_->passed_.empty());

        // Receive one more forward_skip with only peers_[3]'s signature -> all 4 proposers have output, can Pass
        signatures = std::vector<ssz_types::Signature>();
        auto s4 = CreateSkipMessage(1, peers_[3], Signature());
        ssz_types::Signature signature4;
        signature4.node_id = peers_[3];
        signature4.signature = MockSignFunc(3, CalculateSkipDigest(s4));
        signatures.push_back(std::move(signature4));
        forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
        engine_->DoOnRecvForwardSkipMessage(peers_[3], forward_skip);
        EXPECT_EQ(engine_->skipped_.size(), 4);
        EXPECT_EQ(engine_->output_.size(), 4);
        EXPECT_TRUE(!engine_->passed_.empty());

        engine_->output_.clear();
        engine_->DoOnRecvForwardSkipMessage(peers_[1], forward_skip);
        EXPECT_TRUE(engine_->output_.empty());
    }

    void DoOnRecvPassMessageTest() {
        auto consensus_cb = [&](Seq seq,
                                uint64_t ts,
                                Digest digest,
                                std::vector<ssz_types::VoteStatus>& vote_status) {
            engine_->DoUpdateRawSeq(seq, ts, false, false);
            return std::make_pair(false,
                                  std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
        };
        // duplicate update raw
        engine_->consensus_finish_cb_ = consensus_cb;
        auto ts = NOWTIME;
        engine_->last_consensus_ts_.store(ts);

        engine_->DoOnRecvPassMessage(peers_[1], nullptr);
        EXPECT_TRUE(engine_->passed_.empty());

        Seq seq = 1;
        auto pass = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        engine_->DoOnRecvPassMessage(peers_[0], pass);
        EXPECT_EQ(engine_->passed_.size(), 1);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq - 1);
        engine_->DoOnRecvPassMessage(peers_[0], pass);
        EXPECT_EQ(engine_->passed_.size(), 1);

        engine_->DoOnRecvPassMessage(peers_[2], pass);
        EXPECT_EQ(engine_->passed_.size(), 2);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq - 1);
        pass->endorsed.PushBack(peers_[0]);
        engine_->DoOnRecvPassMessage(peers_[1], pass);
        EXPECT_EQ(engine_->passed_.size(), 3);
        EXPECT_EQ(engine_->pending_[1].size(), 1);
        EXPECT_TRUE(engine_->pending_[1].count(peers_[0]));
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq - 1);
        engine_->DoMyBACompleteCallBack(seq, peers_[0], ZERO_32_BYTES);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        EXPECT_EQ(engine_->last_consensus_ts_.load(), ts + 5000);  // empty proposal

        ts = ts + 5000;
        seq = 2;
        // V7+ limited proposer behavior (same as PIPELINE_PROPOSE)
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        pass = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        engine_->DoOnRecvPassMessage(peers_[0], pass);
        EXPECT_EQ(engine_->passed_.size(), 1);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq - 1);
        engine_->DoOnRecvPassMessage(peers_[0], pass);
        EXPECT_EQ(engine_->passed_.size(), 1);

        engine_->DoOnRecvPassMessage(peers_[2], pass);
        EXPECT_EQ(engine_->passed_.size(), 2);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq - 1);
        // not current proposer, will not pending and direct consensus complete
        engine_->current_proposers_.erase(peers_[1]);
        pass->endorsed.PushBack(peers_[1]);
        engine_->DoOnRecvPassMessage(peers_[1], pass);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // empty proposal will add 5000
        EXPECT_EQ(engine_->last_consensus_ts_.load(), ts + 5000);
    }

    void DoUpdateRawSeqTest() {
        engine_->last_consensus_seq_.store(1);

        engine_->DoUpdateRawSeq(2, NOWTIME, false, false);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), 2);

        engine_->DoUpdateRawSeq(3, NOWTIME, false, false);
        EXPECT_EQ(engine_->last_consensus_seq_.load(), 3);



        engine_->Start();
        engine_->UpdateSyncRawSeq(5, NOWTIME, false);
        EXPECT_FALSE(engine_->is_epoch_end_.load());

        engine_->UpdateSyncRawSeq(7, NOWTIME, true);
        while (engine_->last_consensus_seq_.load() != 7) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), 7);
        EXPECT_TRUE(engine_->is_epoch_end_.load());
    }

    void DoUpdateStableSeqTest() {
        Seq seq = 5;
        engine_->last_stable_seq_.store(seq);
        engine_->last_consensus_seq_.store(seq + 1);
        
        // Start engine to enable async operations
        engine_->Start();
        
        // Test normal increment: seq -> seq + 1
        engine_->UpdateStableSeq(seq + 1, std::set<NodeId>());
        while (engine_->last_stable_seq_.load() != seq + 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_stable_seq_.load(), seq + 1);

        // Test case: seq == last_stable + 1 but seq > last_finished_seq_
        // V7+: last_finished_seq_ should be pulled up when stable seq advances
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->last_stable_seq_.store(seq + 1);
        engine_->last_finished_seq_ = seq;  // last_finished_seq_ < seq + 2
        engine_->last_consensus_seq_.store(seq + 2);
        
        engine_->UpdateStableSeq(seq + 2, std::set<NodeId>());
        while (engine_->last_stable_seq_.load() != seq + 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_stable_seq_.load(), seq + 2);
        EXPECT_EQ(engine_->last_finished_seq_, seq + 2);  // last_finished_seq_ should be pulled up to seq + 2

        // Test case: next_proposers non-empty (V7+ limited proposers)
        // Should record next_proposers to next_proposers_[seq + cfg_.proposer_shuffle_window_]
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->last_stable_seq_.store(seq + 2);
        engine_->last_consensus_seq_.store(seq + 3);
        engine_->cfg_.proposer_shuffle_window_ = 10;
        
        // Add some old entries to next_proposers_ that should be cleaned up
        engine_->next_proposers_[seq] = {peers_[0]};
        engine_->next_proposers_[seq + 1] = {peers_[1]};
        
        // Create next_proposers set
        std::set<NodeId> next_proposers = {peers_[2], peers_[3]};
        
        engine_->UpdateStableSeq(seq + 3, next_proposers);
        while (engine_->last_stable_seq_.load() != seq + 3) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_stable_seq_.load(), seq + 3);
        
        // Verify next_proposers_ is updated correctly
        // Should have entry at seq + 3 + proposer_shuffle_window_ = 5 + 3 + 10 = 18
        EXPECT_TRUE(engine_->next_proposers_.count(seq + 3 + engine_->cfg_.proposer_shuffle_window_) > 0);
        EXPECT_EQ(engine_->next_proposers_[seq + 3 + engine_->cfg_.proposer_shuffle_window_], next_proposers);
        
        // Old entries (seq, seq+1, seq+2) should be erased (all <= seq+3)
        EXPECT_EQ(engine_->next_proposers_.count(seq), 0);
        EXPECT_EQ(engine_->next_proposers_.count(seq + 1), 0);
        EXPECT_EQ(engine_->next_proposers_.count(seq + 2), 0);

        // Test case: window was full, now can proceed (line 1118 logic)
        // When seq + cfg_.consensus_window_ <= last_consensus + 1, should call HandlePendingMessage
        // This tests the branch at line 1118: if (seq + cfg_.consensus_window_ <= last_consensus + 1)
        
        // Setup: window is initially full
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->last_stable_seq_.store(10);
        engine_->last_consensus_seq_.store(20);  // last_consensus = 20
        engine_->last_finished_seq_ = 20;  // Important: set last_finished_seq_ for CanPipeline check
        engine_->cfg_.consensus_window_ = 10;
        engine_->is_epoch_end_.store(false);
        
        // Setup check_validity callback to allow message processing
        auto check_validity_callback = [&](Seq seq,
                                           const NodeId& id,
                                           const std::pair<uint32_t, uint32_t> index_epoch,
                                           std::shared_ptr<ABuffer> buffer) {
            engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
            return true;
        };
        engine_->check_validity_ = check_validity_callback;
        
        // Add a pending message for last_consensus + 1 = 21
        Seq pending_seq = 21;
        auto pending_val = CreateValMessage(pending_seq, peers_[0], NOWTIME, asBytes("pending_payload"));
        MessagePtr pending_msg = std::make_shared<ssz_types::ConsensusMessage>(pending_val);
        
        PersistentKey pending_key(peers_[0]);
        MessageGenericInfo pending_info;
        pending_info.seq_ = pending_seq;
        pending_info.proposer_id_ = peers_[0];
        pending_key.msg_info_ = pending_info;
        engine_->pending_msg_.emplace(pending_key, pending_msg);
        
        EXPECT_EQ(engine_->pending_msg_.size(), 1);
        
        // Before UpdateStableSeq(11):
        // - last_stable = 10, last_consensus = 20, last_finished = 20
        // - window check: 10 + 10 = 20 < 20 + 1 = 21, so window is full
        // - CanPipeline(21) = (21 <= 10+10) && (22 <= 20+10) = false && true = false
        EXPECT_TRUE(10 + engine_->cfg_.consensus_window_ < 20 + 1);  // Window was full before
        EXPECT_FALSE(engine_->CanPipeline(pending_seq));  // Cannot process pending message yet
        
        // After UpdateStableSeq(11):
        // - last_stable = 11, last_consensus = 20, last_finished = 20
        // - window check: 11 + 10 = 21 <= 20 + 1 = 21, so window is not full
        // - CanPipeline(21) = (21 <= 11+10) && (22 <= 20+10) = true && true = true
        // - Should trigger HandlePendingMessage(21) at line 1125
        engine_->UpdateStableSeq(11, std::set<NodeId>());
        while (engine_->last_stable_seq_.load() != 11) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_stable_seq_.load(), 11);
        
        // Verify the condition that triggers HandlePendingMessage
        EXPECT_TRUE(11 + engine_->cfg_.consensus_window_ <= 20 + 1);  // Window is not full now
        EXPECT_TRUE(engine_->CanPipeline(pending_seq));  // Can process pending message now
        
        // Wait for HandlePendingMessage to process the pending message
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Verify that the pending message was actually processed and removed
        EXPECT_EQ(engine_->pending_msg_.size(), 0);

        // Test invalid decrement: should cause fatal error
        // Note: EXPECT_DEATH requires direct call to DoUpdateStableSeq
        EXPECT_DEATH(engine_->DoUpdateStableSeq(seq, std::set<NodeId>()), ".*");
        
        // Test invalid jump: should cause fatal error
        engine_->last_stable_seq_.store(seq);
        EXPECT_DEATH(engine_->DoUpdateStableSeq(seq + 3, std::set<NodeId>()), ".*");
        
        // Stop engine after test
        engine_->Stop();
    }

    void DoUpdateFinishedSequenceTest() {
        // Test UpdateFinishedSequence window flow logic
        // Similar to DoUpdateStableSeqTest, but testing the last_finished_seq_ constraint in CanPipeline
        
        // CanPipeline check (V7+): num <= last_stable_seq_ + consensus_window_
        //        && num + 1 <= last_finished_seq_ + consensus_window_;
        // Example: last_stable_seq_ = last_finished_seq_ = 1, last_consensus_ = 10
        // CanPipeline(11) = (11 <= 1+10) && (12 <= 1+10) = true && false = false
        // Must update last_finished_seq_ to 2 to allow CanPipeline(11) = true
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->cfg_.consensus_window_ = 10;
        engine_->is_epoch_end_.store(false);
        
        // Setup initial state
        engine_->last_stable_seq_.store(1);
        engine_->last_finished_seq_ = 1;
        engine_->last_consensus_seq_.store(10);
        
        // Setup check_validity callback to allow message processing
        auto check_validity_callback = [&](Seq seq,
                                           const NodeId& id,
                                           const std::pair<uint32_t, uint32_t> index_epoch,
                                           std::shared_ptr<ABuffer> buffer) {
            engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
            return true;
        };
        engine_->check_validity_ = check_validity_callback;
        
        // Start engine to enable async operations
        engine_->Start();
        
        // Add a pending message for last_consensus + 1 = 11
        Seq pending_seq = 11;
        auto pending_val = CreateValMessage(pending_seq, peers_[0], NOWTIME, asBytes("pending_payload"));
        MessagePtr pending_msg = std::make_shared<ssz_types::ConsensusMessage>(pending_val);
        
        PersistentKey pending_key(peers_[0]);
        MessageGenericInfo pending_info;
        pending_info.seq_ = pending_seq;
        pending_info.proposer_id_ = peers_[0];
        pending_key.msg_info_ = pending_info;
        engine_->pending_msg_.emplace(pending_key, pending_msg);
        
        EXPECT_EQ(engine_->pending_msg_.size(), 1);
        
        // Before UpdateFinishedSequence(2):
        // - last_stable = 1, last_finished = 1, last_consensus = 10
        // - CanPipeline(11) = (11 <= 1+10) && (12 <= 1+10) = true && false = false
        // - Window is blocked by last_finished_seq_
        EXPECT_TRUE(pending_seq <= engine_->last_stable_seq_.load() + engine_->cfg_.consensus_window_);
        EXPECT_FALSE(pending_seq + 1 <= engine_->last_finished_seq_ + engine_->cfg_.consensus_window_);
        EXPECT_FALSE(engine_->CanPipeline(pending_seq));  // Cannot process pending message yet
        
        // After UpdateFinishedSequence(2):
        // - last_stable = 1, last_finished = 2, last_consensus = 10
        // - CanPipeline(11) = (11 <= 1+10) && (12 <= 2+10) = true && true = true
        // - Should trigger HandlePendingMessage(11) at line 1075
        std::vector<BlockProofBuffer> checkpoints = {"checkpoint_1", "checkpoint_2"};
        engine_->UpdateFinishedSequence(2, checkpoints);
        
        // Wait for async operation to complete
        while (engine_->last_finished_seq_ != 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_finished_seq_, 2);
        
        // Verify the condition that triggers HandlePendingMessage
        EXPECT_TRUE(pending_seq <= engine_->last_stable_seq_.load() + engine_->cfg_.consensus_window_);
        EXPECT_TRUE(pending_seq + 1 <= engine_->last_finished_seq_ + engine_->cfg_.consensus_window_);
        EXPECT_TRUE(engine_->CanPipeline(pending_seq));  // Can process pending message now
        
        // Wait for HandlePendingMessage to process the pending message
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Verify that the pending message was actually processed and removed
        EXPECT_EQ(engine_->pending_msg_.size(), 0);
        
        // Verify my_finished_proofs_ was updated
        EXPECT_EQ(engine_->my_finished_proofs_.size(), 1);
        EXPECT_EQ(engine_->my_finished_proofs_[2].size(), 2);
        
        // Stop engine after test
        engine_->Stop();
    }

    void DoOnConsensusMessageTest(const std::vector<NodeId>& ids) {
        engine_->stopped_ = false;
        auto new_check_validity = [&](Seq seq,
                                      const NodeId& id,
                                      const std::pair<uint32_t, uint32_t> index_epoch,
                                      std::shared_ptr<ABuffer> buffer) {
            engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
            return true;
        };
        engine_->check_validity_ = new_check_validity;
        engine_->OnConsensusMessage(ids[0], nullptr);
        MessagePtr invalid_msg = std::make_shared<ssz_types::ConsensusMessage>();
        engine_->OnConsensusMessage(NodeId(std::string(32, '9')), invalid_msg);
        engine_->OnConsensusMessage(ids[0], invalid_msg);

        auto m1 = CreateValReceiptMessage(0, std::string(32, '0'), engine_->epoch_number_);
        engine_->OnConsensusMessage(std::string(32, '0'),
                                    std::make_shared<ssz_types::ConsensusMessage>(m1));
        engine_->OnConsensusMessage(ids[0], std::make_shared<ssz_types::ConsensusMessage>(m1));

        auto req = CreateRequestProposalMessage(0, std::vector<ssz_types::ProposalRequestKey>());
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(req));
        std::vector<std::shared_ptr<ssz_types::ValMessage>> tmp_vals{};
        auto resp = CreateResponseProposalMessage(tmp_vals);
        // empty response proposal message should be rejected
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(resp));

        auto m2 = CreateValMessage(0, std::string(32, '0'), 0, bytes(), engine_->epoch_number_);
        auto msg = std::make_shared<ssz_types::ConsensusMessage>(m2);
        engine_->OnConsensusMessage(ids[1], msg);
        engine_->last_consensus_seq_.store(1);
        auto m3 = CreateValMessage(0, std::string(32, '0'), 0, bytes(), engine_->epoch_number_);
        m3->seq = 1;
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(m3));
        m3->seq = 25;
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(m3));
        EXPECT_TRUE(engine_->pending_msg_.empty());
        m3->seq = 15;
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(m3));
        engine_->unchecked_proposals_.clear();
        EXPECT_TRUE(!engine_->pending_msg_.empty());
        auto p1 = CreatePassMessage(2, std::vector<ssz::ByteVector<32>>());
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(p1));
        auto s1 = CreateSkipMessage(0, std::string(32, '0'), Signature());
        s1->seq = 2;
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(s1));
        std::vector<ssz_types::Signature> signatures;
        auto f1 = CreateForwardSkipMessage(0, std::move(signatures));
        f1->seq = 2;
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(f1));
        auto b1 = CreateBvalMessage(0, std::string(32, '0'), 0, Signature(), false, engine_->epoch_number_);
        b1->seq = 2;
        b1->proposer_id = ids[0];
        engine_->OnConsensusMessage(ids[1], std::make_shared<ssz_types::ConsensusMessage>(b1));
    }

    void DoOnRequestProposalTimeoutTest() {
        std::atomic<int> skip_broadcast_count{0};
        std::atomic<int> request_proposal_broadcast_count{0};
        auto broadcast = [&](const MessagePtr msg) {
            if (msg == nullptr) {
                return;
            }
            if (msg->Index() == ConsensusMessageType::SkipMessage) {
                skip_broadcast_count++;
            } else if (msg->Index() == ConsensusMessageType::RequestProposalMessage) {
                request_proposal_broadcast_count++;
            }
        };
        engine_->broadcast_ = broadcast;

        engine_->DoOnRequestProposalTimeout(asio::error_code(), true);
        EXPECT_EQ(skip_broadcast_count.load(), 0);
        EXPECT_EQ(request_proposal_broadcast_count.load(), 0);

        engine_->DoOnRequestProposalTimeout(asio::error_code(), false);
        // No ba_success_ yet -> no RequestProposalMessage; may get SkipMessage from BroadcastProofs if long time no stable
        EXPECT_EQ(skip_broadcast_count.load(), 1);
        EXPECT_EQ(request_proposal_broadcast_count.load(), 0);

        engine_->ba_success_[std::string(32, '0')] = Digest("1");
        engine_->DoOnRequestProposalTimeout(asio::error_code(), false);
        EXPECT_EQ(skip_broadcast_count.load(), 2);
        EXPECT_GE(request_proposal_broadcast_count.load(), 1);
    }

    void DoOnSkipSendTimeoutTest() {
        EXPECT_EQ(engine_->skipped_.size(), 0);
        engine_->DoOnSkipSendTimeout(asio::error_code(), true);
        EXPECT_EQ(engine_->skipped_.size(), 0);

        // already send skip
        engine_->skipped_.emplace(peers_[0], "signature");
        engine_->DoOnSkipSendTimeout(asio::error_code(), false);
        EXPECT_EQ(engine_->skipped_.size(), 1);

        // in consensus
        engine_->skipped_.clear();
        engine_->propose_state_.is_proposed_.store(true);
        engine_->DoOnSkipSendTimeout(asio::error_code(), false);
        EXPECT_EQ(engine_->skipped_.size(), 0);

        engine_->propose_state_.is_proposed_.store(false);
        engine_->DoOnSkipSendTimeout(asio::error_code(), false);
        EXPECT_TRUE(!engine_->propose_state_.is_proposed_.load());
        EXPECT_EQ(engine_->skipped_.size(), 0);

        engine_->ba_success_[std::string(32, '0')] = Digest("1");
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
        engine_->DoOnSkipSendTimeout(asio::error_code(), false);
        EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());
        EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 1);
        EXPECT_EQ(engine_->skipped_.size(), 1);
    }

    void DoOnForwardSkipTimeoutTest() {
        engine_->DoOnForwardSkipTimeout(asio::error_code(), true);

        engine_->skipped_.emplace(peers_[0], "signature");
        engine_->DoOnForwardSkipTimeout(asio::error_code(), false);
    }

    void DoOnRecvRequestProposalMessageTest() {
        auto req = CreateRequestProposalMessage(0, std::vector<ssz_types::ProposalRequestKey>());
        EXPECT_EQ(engine_->DoOnRecvRequestProposalMessage(std::string(32, '0'), req), -1);

        req->seq = 2;
        engine_->DoUpdateStableSeq(1, std::set<NodeId>());
        EXPECT_EQ(engine_->DoOnRecvRequestProposalMessage(std::string(32, '0'), req), -2);

        ssz_types::ProposalRequestKey k1;
        k1.proposer_id = peers_[1];
        k1.hash = Digest("2");
        req->keys.PushBack(std::move(k1));
        EXPECT_EQ(engine_->DoOnRecvRequestProposalMessage(std::string(32, '0'), req), -3);
        engine_->val_[2][peers_[1]][Digest("2")] =
            CreateValMessage(0, std::string(32, '0'), 0, bytes());
        EXPECT_EQ(engine_->DoOnRecvRequestProposalMessage(std::string(32, '0'), req), 0);
    }

    void DoOnRecvResponseProposalMessageTest() {
        std::vector<std::shared_ptr<ssz_types::ValMessage>> tmp_vals{};
        auto resp = CreateResponseProposalMessage(tmp_vals);
        EXPECT_EQ(engine_->DoOnRecvResponseProposalMessage(std::string(32, '0'), resp), -1);

        auto v1 = CreateValMessage(2, std::string(32, '0'), 10086, asBytes("payload"));
        resp->vals.PushBack(v1);
        EXPECT_EQ(engine_->DoOnRecvResponseProposalMessage(std::string(32, '0'), resp), -2);

        engine_->last_consensus_seq_.store(1);
        engine_->pending_[2].insert(std::string(32, '0'));
        engine_->ba_complete_.emplace(std::string(32, '0'));
        engine_->ba_success_[std::string(32, '0')] = Digest("1");
        engine_->PrintDiagnostics();
        EXPECT_EQ(engine_->DoOnRecvResponseProposalMessage(std::string(32, '0'), resp), -3);

        engine_->ba_success_[std::string(32, '0')] = Digest(v1->hash.Acquire());
        auto check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
            return false;
        };
        engine_->check_validity_ = check_validity;
        EXPECT_EQ(engine_->DoOnRecvResponseProposalMessage(std::string(32, '0'), resp), -4);

        auto new_check_validity = [&](Seq seq,
                                      const NodeId& id,
                                      const std::pair<uint32_t, uint32_t> index_epoch,
                                      std::shared_ptr<ABuffer> buffer) {
            engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
            return true;
        };
        engine_->check_validity_ = new_check_validity;
        engine_->unchecked_proposals_.clear();
        EXPECT_EQ(engine_->DoOnRecvResponseProposalMessage(std::string(32, '0'), resp), 0);
        engine_->PrintDiagnostics();
        EXPECT_TRUE(engine_->pending_[2].empty());
        EXPECT_TRUE(engine_->val_[2][std::string(32, '0')][Digest(v1->hash.Acquire())] == v1);
    }

    void DoCanPipelineTest() {
        // TODO
    }

    void DoCanProposeTest() {
        engine_->propose_state_.SetProposeState(false, 6, 5);
        engine_->stopped_ = true;
        auto ret = engine_->CanPropose();
        EXPECT_EQ(
            ret,
            (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_FAILED_EPOCH_END,
                                                   0)));
        engine_->stopped_ = false;

        engine_->propose_state_.AdvanceProposeState(false, 1);
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_READY, 5)));

        engine_->propose_state_.AdvanceProposeState(false, 11);
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL,
                      0)));

        engine_->last_stable_seq_.store(1);
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
            (std::pair<MyTumblerProposeState, Seq>(
                MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL,
                0)));
        engine_->last_finished_seq_ = 1;
        EXPECT_EQ(ret,
            (std::pair<MyTumblerProposeState, Seq>(
                MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL,
                0)));

        engine_->last_finished_seq_ = 2;
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_READY, 5)));

        engine_->propose_state_.is_proposed_ = true;
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS,
                      0)));

        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->propose_state_.AdvanceProposeState(false, 12);
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL,
                      0)));
        engine_->last_finished_seq_ = 8;
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_WINDOW_FULL,
                      0)));
        engine_->last_stable_seq_.store(2);
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_READY, 5)));
        engine_->propose_state_.is_proposed_ = true;
        ret = engine_->CanPropose();
        EXPECT_EQ(ret,
                  (std::pair<MyTumblerProposeState, Seq>(
                      MyTumblerProposeState::PROPOSE_FAILED_IN_CONSENSUS,
                      0)));
    }

    void DoHandlePendingMessageTest() {
        engine_->stopped_ = false;
        auto new_check_validity = [&](Seq seq,
                                      const NodeId& id,
                                      const std::pair<uint32_t, uint32_t> index_epoch,
                                      std::shared_ptr<ABuffer> buffer) {
            engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
            return true;
        };
        engine_->check_validity_ = new_check_validity;
        auto msg1 = CreateValMessage(5, peers_[1], 0, bytes(), engine_->epoch_number_);
        engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(msg1));
        auto msg2 = CreateValMessage(6, peers_[2], 1, bytes(), engine_->epoch_number_);
        engine_->OnConsensusMessage(peers_[2], std::make_shared<ssz_types::ConsensusMessage>(msg2));
        EXPECT_EQ(engine_->pending_msg_.size(), 2);
        engine_->last_consensus_seq_.store(5);
        engine_->HandlePendingMessage(6);
        EXPECT_TRUE(engine_->pending_msg_.empty());
    }

    void DoOnConsensusCompleteTest() {
        auto val_1 = CreateValMessage(1, peers_[1], 8, bytes());
        Digest hash_1 = Digest(val_1->hash.Acquire());
        engine_->val_[1][peers_[1]][hash_1] = val_1;
        engine_->ba_success_[peers_[1]] = hash_1;
        engine_->DoOnConsensusComplete(1);
        EXPECT_EQ(engine_->last_consensus_ts_.load(), 8);

        auto val_2 = CreateValMessage(1, peers_[2], 6, bytes());
        Digest hash_2 = Digest(val_2->hash.Acquire());
        engine_->val_[1][peers_[2]][hash_2] = val_2;
        engine_->ba_success_[peers_[2]] = hash_2;

        engine_->ba_success_[peers_[1]] = hash_1;
        engine_->last_consensus_ts_.store(0);
        engine_->last_consensus_seq_.store(0);
        engine_->DoOnConsensusComplete(1);
        EXPECT_EQ(engine_->last_consensus_ts_.load(), 8);

        auto val_3 = CreateValMessage(1, peers_[3], 7, bytes());
        Digest hash_3 = Digest(val_3->hash.Acquire());
        engine_->val_[1][peers_[3]][hash_3] = val_3;
        engine_->ba_success_[peers_[3]] = hash_3;

        engine_->ba_success_[peers_[1]] = hash_1;
        engine_->ba_success_[peers_[2]] = hash_2;
        engine_->last_consensus_ts_.store(0);
        engine_->last_consensus_seq_.store(0);
        engine_->DoOnConsensusComplete(1);
        EXPECT_EQ(engine_->last_consensus_ts_.load(), 7);

        // timestamp over 300000
        val_3->timestamp = 400000;
        val_2->timestamp = 500000;
        engine_->ba_success_[peers_[3]] = hash_3;

        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        engine_->ba_success_[peers_[1]] = hash_1;
        engine_->ba_success_[peers_[2]] = hash_2;
        engine_->last_consensus_ts_.store(100);
        engine_->last_consensus_seq_.store(0);
        engine_->DoOnConsensusComplete(1);
        EXPECT_EQ(engine_->last_consensus_ts_.load(), 100 + 300000);

        // timestamp back
        engine_->spec_version_ = CONSENSUS_VERSION_NO_TIMESTAMP_CHECK;
        val_3->timestamp = 4;
        val_2->timestamp = 5;

        engine_->ba_success_[peers_[1]] = hash_1;
        engine_->ba_success_[peers_[2]] = hash_2;
        engine_->ba_success_[peers_[3]] = hash_3;
        engine_->last_consensus_ts_.store(100);
        engine_->last_consensus_seq_.store(0);
        engine_->DoOnConsensusComplete(1);
        EXPECT_EQ(engine_->last_consensus_ts_.load(), 100 + 1);
    }
};

TEST_F(MyTumblerEngineTest, ConfigureTest) {
    engine_->PrintDiagnostics();
    DoConfigureTest();
    engine_->PrintDiagnostics();
}

TEST_F(MyTumblerEngineTest, ConfigureWithMessagesInDBTest1) {
    engine_->PrintDiagnostics();
    DoConfigureWithMessagesInDBTest1();
    engine_->PrintDiagnostics();
}

TEST_F(MyTumblerEngineTest, ConfigureWithMessagesInDBTest2) {
    DoConfigureWithMessagesInDBTest2();
}

TEST_F(MyTumblerEngineTest, ConfigureWithMessagesInDBTest3) {
    // version >= 7, old epoch val and new epoch val
    Seq seq = 1;
    bytes payload = asBytes("payload");
    MyTumblerConfig cc = cc_;

    std::shared_ptr<wal::WalLocalStorage> local_db = CreateWALStorage("log/db_test");

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
    {
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
        engine_->InitCryptoFunc(
            [this](const std::string& data) {
                return "digest";
            },
            ecc_signer,
            dummy_aggregator);
    }
    std::map<Seq, std::set<NodeId>> proposers;
    proposers[0] = {peers_[0], peers_[1], peers_[2], peers_[3]};
    EXPECT_TRUE(engine_->Configure(
        CONSENSUS_VERSION_PIPELINE_PROPOSE,
        1,  // epoch
        my_id_,
        peer_pubkey_map_,
        peer_balance_map_,
        cc,
        0,
        0,
        0,
        false,
        proposers,
        local_db,
        [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
            return;
        },
        [this](const consensus_spec::MessagePtr msg) {
            return;
        },
        nullptr,
        [this](Seq seq,
               uint64_t ts,
               Digest digest,
               std::vector<ssz_types::VoteStatus>& vote_status) {
            return std::make_pair(false,
                                  std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
        },
        [](std::vector<BlockProofBuffer> stable_proofs, std::vector<BlockProofBuffer> checkpoints) {
            return;
        }));
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 0);
    EXPECT_EQ(engine_->propose_state_.propose_seq_, 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

    ValMessagePtr v0 = CreateValMessage(seq, peers_[0], NOWTIME, payload, 1);
    std::vector<std::pair<NodeId, MessagePtr>> persistent_msgs = {
        {peers_[0], std::make_shared<ssz_types::ConsensusMessage>(v0)}};
    MyTumblerEngineTest::InsertLog(engine_, persistent_msgs);

    EXPECT_TRUE(engine_->Configure(
        CONSENSUS_VERSION_PIPELINE_PROPOSE,
        2,  // epoch
        my_id_,
        peer_pubkey_map_,
        peer_balance_map_,
        cc,
        0,
        0,
        0,
        false,
        proposers,
        local_db,
        [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
            return;
        },
        [this](const consensus_spec::MessagePtr msg) {
            return;
        },
        nullptr,
        [this](Seq seq,
               uint64_t ts,
               Digest digest,
               std::vector<ssz_types::VoteStatus>& vote_status) {
            return std::make_pair(false,
                                  std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
        },
        [](std::vector<BlockProofBuffer> stable_proofs, std::vector<BlockProofBuffer> checkpoints) {
            return;
        }));
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 0);
    EXPECT_EQ(engine_->propose_state_.propose_seq_, 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

    bytes new_payload = asBytes("new payload");
    ValMessagePtr v1 = CreateValMessage(seq, peers_[0], NOWTIME, new_payload, 2);
    persistent_msgs = {{peers_[0], std::make_shared<ssz_types::ConsensusMessage>(v1)}};
    MyTumblerEngineTest::InsertLog(engine_, persistent_msgs);

    EXPECT_TRUE(engine_->Configure(
        CONSENSUS_VERSION_PIPELINE_PROPOSE,
        2,  // epoch
        my_id_,
        peer_pubkey_map_,
        peer_balance_map_,
        cc,
        0,
        0,
        0,
        false,
        proposers,
        local_db,
        [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
            return;
        },
        [this](const consensus_spec::MessagePtr msg) {
            return;
        },
        nullptr,
        [this](Seq seq,
               uint64_t ts,
               Digest digest,
               std::vector<ssz_types::VoteStatus>& vote_status) {
            return std::make_pair(false,
                                  std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
        },
        [](std::vector<BlockProofBuffer> stable_proofs, std::vector<BlockProofBuffer> checkpoints) {
            return;
        }));
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 0);
    EXPECT_EQ(engine_->propose_state_.propose_seq_, 1);
    EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());
}

// TEST_F(MyTumblerEngineTest, SyncProposeTest) {
//     DoSyncProposeTest();
// }

// TEST_F(MyTumblerEngineTest, SyncProposeNormalTest) {
//     DoSyncProposeNormalTest();
// }

// TEST_F(MyTumblerEngineTest, SyncEmptyProposeTest) {
//     DoSyncEmptyProposeTest();
// }

TEST_F(MyTumblerEngineTest, AsyncProposeTest) {
    DoAsyncProposeTest();
}

TEST_F(MyTumblerEngineTest, AsyncProposeNormalTest) {
    DoAsyncProposeNormalTest();
}

TEST_F(MyTumblerEngineTest, AsyncEmptyProposeTest) {
    DoAsyncEmptyProposeTest();
}

TEST_F(MyTumblerEngineTest, EndorseCallBackTest) {
    DoEndorseCallBackTest();
}

TEST_F(MyTumblerEngineTest, MyBACompleteCallBackTest) {
    DoMyBACompleteCallBackTest();
}

TEST_F(MyTumblerEngineTest, OnRecvValMessageTest) {
    engine_->PrintDiagnostics();
    DoOnRecvValMessageTest();
    engine_->PrintDiagnostics();
}

TEST_F(MyTumblerEngineTest, OnRecvPassMessageTest) {
    engine_->PrintDiagnostics();
    DoOnRecvPassMessageTest();
    engine_->PrintDiagnostics();
}

TEST_F(MyTumblerEngineTest, OnRecvSkipMessageTest) {
    DoOnRecvSkipMessageTest();
}

TEST_F(MyTumblerEngineTest, OnRecvForwardSkipMessageTest) {
    DoOnRecvForwardSkipMessageTest();
}

TEST_F(MyTumblerEngineTest, UpdateRawSeqTest) {
    DoUpdateRawSeqTest();
}

TEST_F(MyTumblerEngineTest, UpdateStableSeqTest) {
    DoUpdateStableSeqTest();
}

TEST_F(MyTumblerEngineTest, UpdateFinishedSequenceTest) {
    DoUpdateFinishedSequenceTest();
}

TEST_F(MyTumblerEngineTest, OnConsensusMessageTest) {
    DoOnConsensusMessageTest(peers_);
}

TEST_F(MyTumblerEngineTest, OnConsensusMessageStoppedTest) {
    // Test scenario 1: engine already stopped
    engine_->stopped_ = true;
    
    auto val_msg = CreateValMessage(1, peers_[0], NOWTIME, asBytes("payload"));
    auto msg = std::make_shared<ssz_types::ConsensusMessage>(val_msg);
    
    // When engine is stopped, OnConsensusMessage should return early without processing
    engine_->OnConsensusMessage(peers_[0], msg);
    
    // Verify no message was processed
    EXPECT_TRUE(engine_->val_.empty());
    EXPECT_TRUE(engine_->endorsed_.empty());
    EXPECT_TRUE(engine_->unchecked_proposals_.empty());
}

TEST_F(MyTumblerEngineTest, OnConsensusMessageEpochCheckTest) {
    // Test scenario 2: epoch_number check for CONSENSUS_VERSION_PIPELINE_PROPOSE
    engine_->stopped_ = false;
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    engine_->epoch_number_ = 5;
    engine_->last_consensus_seq_.store(0);
    engine_->last_stable_seq_.store(0);
    
    Seq seq = 1;
    uint64_t correct_epoch = 5;
    uint64_t wrong_epoch = 3;
    
    // Test ValMessage with correct epoch_number - should pass epoch check and be queued
    auto val_msg_correct = CreateValMessage(seq, peers_[0], NOWTIME, asBytes("payload"), correct_epoch);
    engine_->OnConsensusMessage(peers_[0], std::make_shared<ssz_types::ConsensusMessage>(val_msg_correct));
    // Message should pass epoch check (either processed or pending)
    
    // Test ValMessage with wrong epoch_number - should be rejected at epoch check
    size_t size_before_wrong = engine_->pending_msg_.size();
    auto val_msg_wrong = CreateValMessage(seq, peers_[1], NOWTIME, asBytes("payload"), wrong_epoch);
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(val_msg_wrong));
    // Message should be rejected, pending_msg_ size should not increase
    EXPECT_EQ(engine_->pending_msg_.size(), size_before_wrong);
    
    // Test ValReceiptMessage with correct epoch_number
    auto val_receipt_correct = CreateValReceiptMessage(seq, peers_[0], correct_epoch);
    engine_->OnConsensusMessage(peers_[0], std::make_shared<ssz_types::ConsensusMessage>(val_receipt_correct));
    // Should pass epoch check and be handled by reliable_channel
    
    // Test ValReceiptMessage with wrong epoch_number - should be rejected
    auto val_receipt_wrong = CreateValReceiptMessage(seq, peers_[1], wrong_epoch);
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(val_receipt_wrong));
    // Should be rejected at epoch check
    
    // Test BvalMessage with correct epoch_number
    Digest hash = Digest("test_hash");
    auto bval_msg_correct = CreateBvalMessage(seq, peers_[0], 0, hash, false, correct_epoch);
    bval_msg_correct->proposer_id = peers_[0];
    size_before_wrong = engine_->pending_msg_.size();
    engine_->OnConsensusMessage(peers_[0], std::make_shared<ssz_types::ConsensusMessage>(bval_msg_correct));
    // Should pass epoch check
    
    // Test BvalMessage with wrong epoch_number - should be rejected
    auto bval_msg_wrong = CreateBvalMessage(seq, peers_[1], 0, hash, false, wrong_epoch);
    bval_msg_wrong->proposer_id = peers_[1];
    size_t size_after_correct = engine_->pending_msg_.size();
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(bval_msg_wrong));
    // Message should be rejected, pending_msg_ size should not increase
    EXPECT_EQ(engine_->pending_msg_.size(), size_after_correct);
    
    // Test BvalReceiptMessage with correct epoch_number
    auto bval_receipt_correct = CreateBvalReceiptMessage(seq, peers_[0], 0, hash, correct_epoch);
    engine_->OnConsensusMessage(peers_[0], std::make_shared<ssz_types::ConsensusMessage>(bval_receipt_correct));
    // Should pass epoch check and be handled by reliable_channel
    
    // Test BvalReceiptMessage with wrong epoch_number - should be rejected
    auto bval_receipt_wrong = CreateBvalReceiptMessage(seq, peers_[1], 0, hash, wrong_epoch);
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(bval_receipt_wrong));
    // Should be rejected at epoch check
}

TEST_F(MyTumblerEngineTest, OnConsensusMessageVariantNullTest) {
    // Try every ConsensusMessage variant with null pointer (no spec_version_ change).
    // Start engine so async handlers run; verify no crash in both sync and async paths.
    engine_->stopped_ = false;
    engine_->last_consensus_seq_.store(0);
    engine_->last_stable_seq_.store(0);
    engine_->Start();

    auto try_variant_null = [this](const MessagePtr& msg, const char* name) {
        size_t pending_before = engine_->pending_msg_.size();
        engine_->OnConsensusMessage(peers_[0], msg);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        EXPECT_EQ(engine_->pending_msg_.size(), pending_before) << "variant " << name;
    };

    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(ValMessagePtr(nullptr)),
                     "ValMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(BvalMessagePtr(nullptr)),
                     "BvalMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(AuxMessagePtr(nullptr)),
                     "AuxMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(PromMessagePtr(nullptr)),
                     "PromMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::PromCertificateMessage_DEPRECATED>(nullptr)),
        "PromCertificateMessage_DEPRECATED");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(PassMessagePtr(nullptr)),
                     "PassMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(SkipMessagePtr(nullptr)),
                     "SkipMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::TermMessage_DEPRECATED>(nullptr)),
        "TermMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::DKGFinalStatusMessage_DEPRECATED>(nullptr)),
        "DKGFinalStatusMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedBvalMessagePtr(nullptr)),
        "AggregatedBvalMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedMainVoteMessagePtr(nullptr)),
        "AggregatedMainVoteMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ValReceiptMessagePtr(nullptr)),
        "ValReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(BvalReceiptMessagePtr(nullptr)),
        "BvalReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AuxReceiptMessagePtr(nullptr)),
        "AuxReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(PromReceiptMessagePtr(nullptr)),
        "PromReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::PromCertificateReceiptMessage_DEPRECATED>(nullptr)),
        "PromCertificateReceiptMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(PassReceiptMessagePtr(nullptr)),
        "PassReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(SkipReceiptMessagePtr(nullptr)),
        "SkipReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::TermReceiptMessage>(nullptr)),
        "TermReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::DKGFinalStatusReceiptMessage_DEPRECATED>(nullptr)),
        "DKGFinalStatusReceiptMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedBvalReceiptMessagePtr(nullptr)),
        "AggregatedBvalReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedMainVoteReceiptMessagePtr(nullptr)),
        "AggregatedMainVoteReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(RequestProposalMessagePtr(nullptr)),
        "RequestProposalMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ResponseProposalMessagePtr(nullptr)),
        "ResponseProposalMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::DKGAlreadyFinishMessage_DEPRECATED>(nullptr)),
        "DKGAlreadyFinishMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ForwardSkipMessagePtr(nullptr)),
        "ForwardSkipMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ForwardSkipReceiptMessagePtr(nullptr)),
        "ForwardSkipReceiptMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(),
                     "default_constructed");

    engine_->Stop();
}

TEST_F(MyTumblerEngineTest, OnConsensusMessageVariantNullTest_OldVersion) {
    // Same as above but with spec_version_ = CONSENSUS_VERSION_MIN (not fixture default) to
    // exercise null-variant handling at the supported minimum version.
    engine_->stopped_ = false;
    engine_->spec_version_ = CONSENSUS_VERSION_MIN;
    engine_->last_consensus_seq_.store(0);
    engine_->last_stable_seq_.store(0);
    engine_->Start();

    auto try_variant_null = [this](const MessagePtr& msg, const char* name) {
        size_t pending_before = engine_->pending_msg_.size();
        engine_->OnConsensusMessage(peers_[0], msg);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        EXPECT_EQ(engine_->pending_msg_.size(), pending_before) << "variant " << name;
    };

    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(ValMessagePtr(nullptr)),
                     "ValMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(BvalMessagePtr(nullptr)),
                     "BvalMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(AuxMessagePtr(nullptr)),
                     "AuxMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(PromMessagePtr(nullptr)),
                     "PromMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::PromCertificateMessage_DEPRECATED>(nullptr)),
        "PromCertificateMessage_DEPRECATED");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(PassMessagePtr(nullptr)),
                     "PassMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(SkipMessagePtr(nullptr)),
                     "SkipMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::TermMessage_DEPRECATED>(nullptr)),
        "TermMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::DKGFinalStatusMessage_DEPRECATED>(nullptr)),
        "DKGFinalStatusMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedBvalMessagePtr(nullptr)),
        "AggregatedBvalMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedMainVoteMessagePtr(nullptr)),
        "AggregatedMainVoteMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ValReceiptMessagePtr(nullptr)),
        "ValReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(BvalReceiptMessagePtr(nullptr)),
        "BvalReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AuxReceiptMessagePtr(nullptr)),
        "AuxReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(PromReceiptMessagePtr(nullptr)),
        "PromReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::PromCertificateReceiptMessage_DEPRECATED>(nullptr)),
        "PromCertificateReceiptMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(PassReceiptMessagePtr(nullptr)),
        "PassReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(SkipReceiptMessagePtr(nullptr)),
        "SkipReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::TermReceiptMessage>(nullptr)),
        "TermReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::DKGFinalStatusReceiptMessage_DEPRECATED>(nullptr)),
        "DKGFinalStatusReceiptMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedBvalReceiptMessagePtr(nullptr)),
        "AggregatedBvalReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(AggregatedMainVoteReceiptMessagePtr(nullptr)),
        "AggregatedMainVoteReceiptMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(RequestProposalMessagePtr(nullptr)),
        "RequestProposalMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ResponseProposalMessagePtr(nullptr)),
        "ResponseProposalMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(
            std::shared_ptr<ssz_types::DKGAlreadyFinishMessage_DEPRECATED>(nullptr)),
        "DKGAlreadyFinishMessage_DEPRECATED");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ForwardSkipMessagePtr(nullptr)),
        "ForwardSkipMessage");
    try_variant_null(
        std::make_shared<ssz_types::ConsensusMessage>(ForwardSkipReceiptMessagePtr(nullptr)),
        "ForwardSkipReceiptMessage");
    try_variant_null(std::make_shared<ssz_types::ConsensusMessage>(),
                     "default_constructed");

    engine_->Stop();
}

TEST_F(MyTumblerEngineTest, OnRequestProposalTimeoutTest) {
    DoOnRequestProposalTimeoutTest();
}

TEST_F(MyTumblerEngineTest, OnSkipSendTimeoutTest) {
    DoOnSkipSendTimeoutTest();
}

TEST_F(MyTumblerEngineTest, OnForwardSkipTimeoutTest) {
    DoOnForwardSkipTimeoutTest();
}

TEST_F(MyTumblerEngineTest, OnRecvRequestProposalMessageTest) {
    DoOnRecvRequestProposalMessageTest();
}

TEST_F(MyTumblerEngineTest, OnRecvResponseProposalMessageTest) {
    DoOnRecvResponseProposalMessageTest();
}

TEST_F(MyTumblerEngineTest, CanProposeTest) {
    DoCanProposeTest();
}

TEST_F(MyTumblerEngineTest, HandlePendingMessageTest) {
    DoHandlePendingMessageTest();
}

TEST_F(MyTumblerEngineTest, DoOnConsensusCompleteTest) {
    DoOnConsensusCompleteTest();
}

TEST_F(MyTumblerEngineTest, CheckMessageSanity) {
    // check validity
    int count = 0;
    int last_index = 0;
    auto check_validity = [&count, &last_index](Seq seq,
                                                const NodeId& id,
                                                const std::pair<uint32_t, uint32_t> index_epoch,
                                                std::shared_ptr<ABuffer> buffer) {
        ++count;
        last_index = index_epoch.first;
        return true;
    };
    engine_->check_validity_ = check_validity;
    EXPECT_FALSE(
        engine_->CheckMessageSanity(1, "test_id", CheckSanityType::SELF_PROPOSED, nullptr));

    ValMessagePtr val = CreateValMessage(1, peers_[0], 1234567, asBytes("payload"));
    EXPECT_TRUE(engine_->CheckMessageSanity(1, peers_[0], CheckSanityType::SELF_PROPOSED, val));
    EXPECT_EQ(count, 1);
    EXPECT_EQ(last_index, 1);
    EXPECT_EQ(engine_->unchecked_proposals_.size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).at(peers_[0]).size(), 1);
    EXPECT_TRUE(engine_->unchecked_proposals_.at(1).at(peers_[0])[0]
                == std::make_pair(CheckSanityType::SELF_PROPOSED, val));

    // duplicated val
    EXPECT_TRUE(engine_->CheckMessageSanity(1, peers_[0], CheckSanityType::SELF_PROPOSED, val));
    EXPECT_EQ(count, 1);
    EXPECT_EQ(last_index, 1);
    EXPECT_EQ(engine_->unchecked_proposals_.size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).at(peers_[0]).size(), 1);
    EXPECT_TRUE(engine_->unchecked_proposals_.at(1).at(peers_[0])[0]
                == std::make_pair(CheckSanityType::SELF_PROPOSED, val));

    // same content val
    ValMessagePtr val2 = CreateValMessage(1, peers_[0], 1234567, asBytes("payload"));
    EXPECT_TRUE(engine_->CheckMessageSanity(1, peers_[0], CheckSanityType::SELF_PROPOSED, val2));
    EXPECT_EQ(count, 1);
    EXPECT_EQ(last_index, 1);
    EXPECT_EQ(engine_->unchecked_proposals_.size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).at(peers_[0]).size(), 1);
    EXPECT_TRUE(engine_->unchecked_proposals_.at(1).at(peers_[0])[0]
                == std::make_pair(CheckSanityType::SELF_PROPOSED, val));

    // receive from others
    ValMessagePtr val3 = CreateValMessage(1, peers_[1], 9999999, asBytes("payload"));
    EXPECT_TRUE(
        engine_->CheckMessageSanity(1, peers_[1], CheckSanityType::RECEIVED_FROM_OTHERS, val3));
    EXPECT_EQ(count, 2);
    EXPECT_EQ(last_index, 1);
    EXPECT_EQ(engine_->unchecked_proposals_.size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).size(), 2);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).at(peers_[1]).size(), 1);
    EXPECT_TRUE(engine_->unchecked_proposals_.at(1).at(peers_[1])[0]
                == std::make_pair(CheckSanityType::RECEIVED_FROM_OTHERS, val3));
    // same content, different type
    ValMessagePtr val4 = CreateValMessage(1, peers_[1], 9999999, asBytes("payload"));
    EXPECT_TRUE(
        engine_->CheckMessageSanity(1, peers_[1], CheckSanityType::PULLED_FROM_OTHERS, val4));
    EXPECT_EQ(count, 3);
    EXPECT_EQ(last_index, 2);
    EXPECT_EQ(engine_->unchecked_proposals_.size(), 1);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).size(), 2);
    EXPECT_EQ(engine_->unchecked_proposals_.at(1).at(peers_[1]).size(), 2);
    EXPECT_TRUE(engine_->unchecked_proposals_.at(1).at(peers_[1])[1]
                == std::make_pair(CheckSanityType::PULLED_FROM_OTHERS, val4));
}

TEST_F(MyTumblerEngineTest, OnCheckSanityDoneTest) {
    engine_->DoOnCheckSanityDone(0,
                                 "test_id",
                                 std::make_pair(1, engine_->epoch_number_),
                                 std::nullopt);  // outdated
    engine_->DoOnCheckSanityDone(1,
                                 "test_id",
                                 std::make_pair(1, engine_->epoch_number_),
                                 std::nullopt);  // not found
    engine_->DoOnCheckSanityDone(1,
                                 "test_id",
                                 std::make_pair(1, engine_->epoch_number_ - 1),
                                 std::nullopt);  // epoch not match

    // self proposed
    ValMessagePtr val = CreateValMessage(1, peers_[0], 1234567, asBytes("payload"));
    ValMessagePtr val2 = CreateValMessage(1, peers_[0], 10086, asBytes("payload2"));
    auto proposal_pair = std::make_pair(CheckSanityType::SELF_PROPOSED, val);
    engine_->unchecked_proposals_[1][peers_[0]].emplace_back(proposal_pair);
    engine_->DoOnCheckSanityDone(1,
                                 peers_[0],
                                 std::make_pair(2, engine_->epoch_number_),
                                 std::nullopt);  // wrong index
    engine_->DoOnCheckSanityDone(1,
                                 peers_[0],
                                 std::make_pair(1, engine_->epoch_number_),
                                 std::nullopt);
    EXPECT_EQ(*(engine_->val_[1][peers_[0]][Digest(val->hash.Acquire())]), *(val));
    EXPECT_NE(*(engine_->val_[1][peers_[0]][Digest(val->hash.Acquire())]), *(val2));
    EXPECT_EQ(engine_->endorsed_[1].count(peers_[0]), 1);
    EXPECT_EQ(engine_->pending_[1].count(peers_[0]), 1);

    // modified proposal
    proposal_pair = std::make_pair(CheckSanityType::SELF_PROPOSED, val2);
    engine_->unchecked_proposals_[1][peers_[0]].emplace_back(proposal_pair);
    Digest hash2 = Digest(val2->hash.Acquire());
    engine_->DoOnCheckSanityDone(1,
                                 peers_[0],
                                 std::make_pair(2, engine_->epoch_number_),
                                 asBytes("new payload"));
    EXPECT_EQ(std::string(val2->payload.Acquire()), "new payload");
    EXPECT_NE(std::string(val2->payload.Acquire()), "payload2");
    EXPECT_EQ(*(engine_->val_[1][peers_[0]][Digest(val2->hash.Acquire())]), *(val2));
    EXPECT_NE(*(engine_->val_[1][peers_[0]][Digest(val2->hash.Acquire())]), *(val));
    EXPECT_EQ(engine_->val_[1][peers_[0]].size(), 2);
}

TEST_F(MyTumblerEngineTest, EpochEndTest) {
    engine_->is_epoch_end_.store(true);
    std::atomic<int> handle_proof_count = 0;
    engine_->handle_block_proof_cb_ = [&handle_proof_count](
                                          std::vector<BlockProofBuffer> stable_proofs,
                                          std::vector<BlockProofBuffer> checkpoints) {
        ++handle_proof_count;
    };
    auto check_validity = [](Seq seq,
                             const NodeId& id,
                             const std::pair<uint32_t, uint32_t> index_epoch,
                             std::shared_ptr<ABuffer> buffer) {
        abort();
        return true;
    };
    engine_->check_validity_ = check_validity;

    engine_->Start();

    // V7+: proof can be in Pass and in Skip (special case); ForwardSkip and Val do not carry proof
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    SkipMessagePtr skip = CreateSkipMessage(1, peers_[1], Signature());
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(skip));
    do {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (handle_proof_count.load() != 1);
    EXPECT_EQ(handle_proof_count.load(), 1);  // Skip may carry proof -> callback invoked

    std::vector<ssz_types::Signature> signatures;
    auto forward_skip = CreateForwardSkipMessage(1, std::move(signatures));
    engine_->OnConsensusMessage(peers_[1],
                                std::make_shared<ssz_types::ConsensusMessage>(forward_skip));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count.load(), 1);  // ForwardSkip does not carry proof

    auto v1 = CreateValMessage(1, peers_[1], NOWTIME, bytes());
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(v1));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count.load(), 1);  // Val does not carry proof

    // Pass also carries proof in V7+
    auto pass = CreatePassMessage(1, std::vector<ssz::ByteVector<32>>());
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(pass));
    do {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (handle_proof_count.load() != 2);
    EXPECT_EQ(handle_proof_count.load(), 2);
    engine_->PrintDiagnostics();
}

TEST_F(MyTumblerEngineTest, BroadcastProofTest) {
    std::atomic<int> broadcast_count = 0;
    auto broadcast = [&broadcast_count](const MessagePtr msg) {
        if (msg->Index() == ConsensusMessageType::SkipMessage) {
            auto skip_msg = msg->SkipData();
            EXPECT_EQ(skip_msg->seq, 0);
            ++broadcast_count;
        }
    };
    engine_->broadcast_ = broadcast;

    engine_->request_proposal_timer_.ResetTimeoutInterval(50);
    engine_->last_stable_update_time_ = NOWTIME + 10 * 1000;
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    engine_->Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_EQ(broadcast_count, 0);
    engine_->last_stable_update_time_ = NOWTIME - 5000;

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    EXPECT_TRUE(broadcast_count > 0);

    engine_->Stop();
}

TEST_F(MyTumblerEngineTest, ReceiveProofTest) {
    std::atomic<int> handle_proof_count = 0;
    engine_->handle_block_proof_cb_ = [&handle_proof_count](
                                          std::vector<BlockProofBuffer> stable_proofs,
                                          std::vector<BlockProofBuffer> checkpoints) {
        ++handle_proof_count;
    };
    engine_->last_consensus_seq_ = 10;
    engine_->last_finished_seq_ = 8;
    engine_->last_stable_seq_ = 5;
    engine_->Start();
    // V7+: proof can be in Pass and in Skip (special case); both trigger handle_block_proof_cb_
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;

    SkipMessagePtr skip_msg = CreateSkipMessage(0, peers_[1], Signature());
    std::vector<ssz::ByteVector<32>> tmp_endorsed;
    auto pass_msg = CreatePassMessage(10, std::move(tmp_endorsed));

    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count, 1);  // Skip may carry proof -> callback invoked

    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count, 2);  // Pass also carries proof

    skip_msg->seq = 5;  // seq <= last_stable_seq_(5): only seq=0 Skip is special, so no proof handling
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count, 2);  // unchanged

    skip_msg->seq = 0;
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count, 3);
    skip_msg->seq = 5;  // again: seq <= last_stable_seq_, no proof
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count, 3);
    engine_->OnConsensusMessage(peers_[1], std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(handle_proof_count, 4);

    engine_->Stop();
}

TEST_F(MyTumblerEngineTest, ReceiveSkipLastBeforeV7Test) {
    // send skip, then immediately send pass, then consensus complete
    auto new_check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
        engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
        return true;
    };

    engine_->check_validity_ = new_check_validity;
    engine_->handle_block_proof_cb_ = [](std::vector<BlockProofBuffer> stable_proofs,
                                         std::vector<BlockProofBuffer> checkpoints) {};
    engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
    engine_->is_proposer_ = true;
    engine_->skip_send_timer_.ResetTimeoutInterval(200);
    engine_->soft_pass_timer_.ResetTimeoutInterval(1000);
    engine_->Start();
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;

    for (int i = 1; i <= 2; ++i) {
        SkipMessagePtr skip_msg = CreateSkipMessage(1, peers_[i], Signature());
        skip_msg->signature = MockSignFunc(i, CalculateSkipDigest(skip_msg));
        engine_->OnConsensusMessage(peers_[i],
                                    std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
        std::vector<ssz::ByteVector<32>> tmp_endorsed;
        auto pass_msg = CreatePassMessage(1, std::move(tmp_endorsed));
        engine_->OnConsensusMessage(peers_[i],
                                    std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
    }

    bytes payload = asBytes("payload");
    auto v3 = CreateValMessage(1, peers_[3], NOWTIME, payload);
    Signature tmp_sig;
    engine_->crypto_helper_->Sign(CalculateBvalDigestForValMessage(v3, engine_->spec_version_),
                                  tmp_sig);
    v3->signature = tmp_sig;
    engine_->DoOnRecvValMessage(peers_[3], v3, RECEIVED_FROM_OTHERS);
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 0);
    engine_->MyBACompleteCallBack(1, peers_[3], Digest(v3->hash.Acquire()));
    while (engine_->last_consensus_seq_.load() != 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
}

TEST_F(MyTumblerEngineTest, ReceiveSkipLastAfterV7Test) {
    // send skip, then immediately send pass, then consensus complete
    auto new_check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
        engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
        return true;
    };

    engine_->check_validity_ = new_check_validity;
    engine_->handle_block_proof_cb_ = [](std::vector<BlockProofBuffer> stable_proofs,
                                         std::vector<BlockProofBuffer> checkpoints) {};
    engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
    engine_->is_proposer_ = true;
    engine_->skip_send_timer_.ResetTimeoutInterval(200);
    engine_->soft_pass_timer_.ResetTimeoutInterval(1000);
    engine_->Start();
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;

    for (int i = 1; i <= 2; ++i) {
        SkipMessagePtr skip_msg = CreateSkipMessage(1, peers_[i], Signature());
        skip_msg->signature = MockSignFunc(i, CalculateSkipDigest(skip_msg));
        engine_->OnConsensusMessage(peers_[i],
                                    std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
        std::vector<ssz::ByteVector<32>> tmp_endorsed;
        auto pass_msg = CreatePassMessage(1, std::move(tmp_endorsed));
        engine_->OnConsensusMessage(peers_[i],
                                    std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
    }

    bytes payload = asBytes("payload");
    auto v3 = CreateValMessage(1, peers_[3], NOWTIME, payload);
    Signature tmp_sig;
    engine_->crypto_helper_->Sign(CalculateBvalDigestForValMessage(v3, engine_->spec_version_),
                                  tmp_sig);
    v3->signature = tmp_sig;
    engine_->DoOnRecvValMessage(peers_[3], v3, RECEIVED_FROM_OTHERS);
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 0);
    engine_->MyBACompleteCallBack(1, peers_[3], Digest(v3->hash.Acquire()));
    while (engine_->last_consensus_seq_.load() != 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
}

TEST_F(MyTumblerEngineTest, CheckCanSkipTest) {
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    engine_->handle_block_proof_cb_ = [](std::vector<BlockProofBuffer> stable_proofs,
                                         std::vector<BlockProofBuffer> checkpoints) {};
    engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
    engine_->is_proposer_ = true;
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
    EXPECT_TRUE(engine_->skipped_.empty());

    engine_->CheckCanSkip();
    EXPECT_TRUE(engine_->skipped_.empty());

    engine_->can_skip_ = true;
    engine_->ba_success_.emplace(peers_[1], "digest");
    // this sequence, already proposed
    engine_->propose_state_.is_proposed_.store(true);
    engine_->CheckCanSkip();
    EXPECT_TRUE(engine_->skipped_.empty());
    // next sequence, not proposed
    engine_->propose_state_.propose_seq_.store(2);
    engine_->propose_state_.is_proposed_.store(false);
    engine_->CheckCanSkip();
    EXPECT_TRUE(engine_->skipped_.empty());
    // next sequence, proposed
    engine_->propose_state_.propose_seq_.store(2);
    engine_->propose_state_.is_proposed_.store(true);
    engine_->CheckCanSkip();
    EXPECT_TRUE(engine_->skipped_.empty());
    // this sequence, not proposed
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(false);
    engine_->CheckCanSkip();
    EXPECT_FALSE(engine_->skipped_.empty());
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 1);
    EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());

    // PIPELINE_PROPOSE: after skip, propose_seq stays 1 until Pass()
    engine_->skipped_.clear();
    // this sequence, not proposed, then skip -> Pass advances to next seq
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(false);
    engine_->CheckCanSkip();
    EXPECT_FALSE(engine_->skipped_.empty());
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 1);
    engine_->Pass();
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
}

TEST_F(MyTumblerEngineTest, ProposeStateAfterPassTest) {
    engine_->handle_block_proof_cb_ = [](std::vector<BlockProofBuffer> stable_proofs,
                                         std::vector<BlockProofBuffer> checkpoints) {};
    // version v7, not yet proposed, will go to next seq
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(false);
    engine_->Pass();
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
    // already proposed, not yet myba done/skip -> unchanged
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(true);
    engine_->Pass();
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 1);
    EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());

    // already proposed and myba succeeded -> advance on Pass()
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(true);
    engine_->ba_complete_.clear();
    engine_->ba_success_.clear();
    engine_->ba_complete_.insert(engine_->my_id_);
    engine_->ba_success_[engine_->my_id_] = std::string(32, '1');  // non-zero = myba succeeded
    engine_->Pass();
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

    // already proposed and myba failed -> advance on Pass()
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(true);
    engine_->ba_complete_.clear();
    engine_->ba_success_.clear();
    engine_->ba_complete_.insert(engine_->my_id_);
    engine_->Pass();
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());

    // already proposed and skipped -> advance on Pass()
    engine_->propose_state_.propose_seq_.store(1);
    engine_->propose_state_.is_proposed_.store(true);
    engine_->ba_complete_.clear();
    engine_->ba_success_.clear();
    engine_->skipped_.emplace(engine_->my_id_, "signature");
    engine_->Pass();
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
}


TEST_F(MyTumblerEngineTest, ResendProofTimeoutTest) {
    // Test DoOnResendProofTimeout triggered by resend_proof_timer_
    // This test verifies:
    // 1. Timer is reset in DoUpdateRawSeq when epoch ends
    // 2. Timer triggers DoOnResendProofTimeout which calls BroadcastProofs
    // 3. Timeout interval increases by 50ms each time, capped at 500ms
    
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    
    // Setup initial state
    engine_->last_consensus_seq_.store(5);
    engine_->last_stable_seq_.store(3);
    engine_->last_finished_seq_ = 4;
    engine_->last_sent_proof_seq_ = 2;
    
    // IMPORTANT: Set is_epoch_end_ to true to enable resend_proof_timer_
    engine_->is_epoch_end_.store(true);
    
    // Create a vector to capture broadcast messages from broadcast_ function
    std::vector<MessagePtr> broadcast_msgs;
    
    // Mock broadcast_ function to capture messages
    engine_->broadcast_ = [&broadcast_msgs](const MessagePtr msg) {
        broadcast_msgs.push_back(msg);
    };
    
    // Add some proofs
    engine_->stable_proofs_[1] = "stable_proof_1";
    engine_->stable_proofs_[2] = "stable_proof_2";
    engine_->stable_proofs_[3] = "stable_proof_3";
    
    engine_->my_finished_proofs_[3] = {"finished_proof_3_1"};
    engine_->my_finished_proofs_[4] = {"finished_proof_4_1", "finished_proof_4_2"};
    
    // Get initial timeout interval
    uint32_t initial_timeout = engine_->cfg_.broadcast_proof_interval_;
    
    // Start the engine - this will start resend_proof_timer_ because is_epoch_end_ is true
    // Start() will call resend_proof_timer_.ResetTimeoutInterval(cfg_.broadcast_proof_interval_)
    engine_->Start();
    
    // Wait a bit for Start() to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Verify initial timeout interval after Start()
    EXPECT_EQ(engine_->resend_proof_timer_.GetTimeoutInterval(), initial_timeout);
    
    // Test Case 1: Wait for first timer timeout (automatically triggered)
    {
        // Wait for timer to trigger (initial_timeout + small buffer to ensure it triggers)
        std::this_thread::sleep_for(std::chrono::milliseconds(initial_timeout + 50));
        
        // Verify BroadcastProofs was called by checking broadcast messages
        ASSERT_FALSE(broadcast_msgs.empty()) << "No broadcast messages after first timeout";
        
        // Find the SkipMessage (BroadcastProofs sends a SkipMessage with seq=0)
        SkipMessagePtr skip = nullptr;
        for (const auto& msg : broadcast_msgs) {
            if (msg->Index() == ConsensusMessageType::SkipMessage) {
                skip = msg->SkipData();
                if (skip && skip->seq == 0) {
                    break;
                }
            }
        }
        ASSERT_NE(skip, nullptr) << "SkipMessage with seq=0 not found after first timeout";
        
        // Verify proofs were added
        EXPECT_GT(skip->encoded_stable_proof.Size(), 0);
        EXPECT_GT(skip->encoded_block_checkpoint_info.Size(), 0);
        
        // Verify timeout interval increased by 50ms
        uint32_t expected_timeout = std::min(initial_timeout + 50, 500u);
        EXPECT_EQ(engine_->resend_proof_timer_.GetTimeoutInterval(), expected_timeout);
        
        broadcast_msgs.clear();
    }
    
    // Test Case 2: Verify timeout backoff continues
    {
        uint32_t current_timeout = engine_->resend_proof_timer_.GetTimeoutInterval();
        
        // Wait for second timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(current_timeout + 50));
        
        // Verify BroadcastProofs was called again
        ASSERT_FALSE(broadcast_msgs.empty()) << "No broadcast messages after second timeout";
        
        // Find the SkipMessage
        SkipMessagePtr skip = nullptr;
        for (const auto& msg : broadcast_msgs) {
            if (msg->Index() == ConsensusMessageType::SkipMessage) {
                skip = msg->SkipData();
                if (skip && skip->seq == 0) {
                    break;
                }
            }
        }
        ASSERT_NE(skip, nullptr) << "SkipMessage with seq=0 not found after second timeout";
        
        // Verify timeout interval increased again by 50ms, but capped at 500ms
        uint32_t expected_timeout = std::min(current_timeout + 50, 500u);
        EXPECT_EQ(engine_->resend_proof_timer_.GetTimeoutInterval(), expected_timeout);
        
        broadcast_msgs.clear();
    }
    
    // Test Case 3: Verify timeout is capped at 500ms
    {
        // Manually set timeout to near the cap
        engine_->resend_proof_timer_.ResetTimeoutInterval(480);
        engine_->resend_proof_timer_.Reset();
        
        // Wait for timeout
        std::this_thread::sleep_for(std::chrono::milliseconds(530));
        
        // Verify timeout is capped at 500ms
        EXPECT_EQ(engine_->resend_proof_timer_.GetTimeoutInterval(), 500u);
        
        // Verify BroadcastProofs was still called
        ASSERT_FALSE(broadcast_msgs.empty()) << "No broadcast messages after third timeout";
        
        broadcast_msgs.clear();
    }
    
    // Test Case 4: Verify timer reset when DoUpdateRawSeq is called with is_epoch_end transition
    {
        // First set is_epoch_end_ to false
        engine_->is_epoch_end_.store(false);
        
        // Then call DoUpdateRawSeq with is_epoch_end = true to trigger the reset
        // This simulates the transition from not epoch end to epoch end
        engine_->DoUpdateRawSeq(6, NOWTIME, false, true);
        
        // Wait a bit for the reset to complete
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        
        // Verify timeout interval is reset to initial value
        EXPECT_EQ(engine_->resend_proof_timer_.GetTimeoutInterval(), initial_timeout);
        EXPECT_TRUE(engine_->is_epoch_end_.load());
    }
    
    // Stop the engine
    engine_->Stop();
}

TEST_F(MyTumblerEngineTest, GetLeaderTest) {
    // Test GetLeader method with V7+ (limited proposers from current_proposers_)
    // Version < 7 is no longer supported; only current_proposers_-based leader is tested.
    
    // Test Case 1: V7+ with limited proposers (2 proposers)
    {
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        
        // Setup current_proposers_ (subset of peers, e.g., 2 proposers)
        engine_->current_proposers_.clear();
        engine_->current_proposers_.insert(peers_[0]);
        engine_->current_proposers_.insert(peers_[2]);
        
        ASSERT_EQ(engine_->current_proposers_.size(), 2);
        
        // Test different sequences
        // seq % node_size determines the leader index
        // seq=0: 0 % 2 = 0 -> first proposer
        // seq=1: 1 % 2 = 1 -> second proposer
        // seq=2: 2 % 2 = 0 -> first proposer (cycle back)
        
        NodeId leader0 = engine_->GetLeader(0);
        NodeId leader1 = engine_->GetLeader(1);
        NodeId leader2 = engine_->GetLeader(2);
        NodeId leader3 = engine_->GetLeader(3);
        
        // Verify leaders rotate through current_proposers_
        // Note: std::set is ordered, so we need to get the actual order
        auto it = engine_->current_proposers_.begin();
        NodeId first_proposer = *it;
        ++it;
        NodeId second_proposer = *it;
        
        EXPECT_EQ(leader0, first_proposer);
        EXPECT_EQ(leader1, second_proposer);
        EXPECT_EQ(leader2, first_proposer);   // Cycle back
        EXPECT_EQ(leader3, second_proposer);
        
        // Test with larger sequence numbers
        NodeId leader100 = engine_->GetLeader(100);
        EXPECT_EQ(leader100, first_proposer);  // 100 % 2 = 0
        
        NodeId leader101 = engine_->GetLeader(101);
        EXPECT_EQ(leader101, second_proposer);  // 101 % 2 = 1
    }
    
    // Test Case 3: Edge case - single proposer
    {
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        
        // Setup current_proposers_ with only one proposer
        engine_->current_proposers_.clear();
        engine_->current_proposers_.insert(peers_[1]);
        
        ASSERT_EQ(engine_->current_proposers_.size(), 1);
        
        // All sequences should return the same leader
        NodeId leader0 = engine_->GetLeader(0);
        NodeId leader1 = engine_->GetLeader(1);
        NodeId leader10 = engine_->GetLeader(10);
        NodeId leader100 = engine_->GetLeader(100);
        
        EXPECT_EQ(leader0, peers_[1]);
        EXPECT_EQ(leader1, peers_[1]);
        EXPECT_EQ(leader10, peers_[1]);
        EXPECT_EQ(leader100, peers_[1]);
    }
    
    // Test Case 4: All peers as proposers
    {
        engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        
        // Setup current_proposers_ with all peers
        engine_->current_proposers_.clear();
        for (const auto& peer : peers_) {
            engine_->current_proposers_.insert(peer);
        }
        
        ASSERT_EQ(engine_->current_proposers_.size(), 4);
        
        // Test rotation through all proposers
        NodeId leader0 = engine_->GetLeader(0);
        NodeId leader1 = engine_->GetLeader(1);
        NodeId leader2 = engine_->GetLeader(2);
        NodeId leader3 = engine_->GetLeader(3);
        NodeId leader4 = engine_->GetLeader(4);
        
        // Get the ordered proposers from the set
        std::vector<NodeId> ordered_proposers(engine_->current_proposers_.begin(),
                                               engine_->current_proposers_.end());
        
        EXPECT_EQ(leader0, ordered_proposers[0]);
        EXPECT_EQ(leader1, ordered_proposers[1]);
        EXPECT_EQ(leader2, ordered_proposers[2]);
        EXPECT_EQ(leader3, ordered_proposers[3]);
        EXPECT_EQ(leader4, ordered_proposers[0]);  // Cycle back
    }
}

TEST_F(MyTumblerEngineTest, RemovePendingInCheckPassStatusTest) {
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    auto new_check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
        engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
        return true;
    };

    engine_->check_validity_ = new_check_validity;

    for (auto& peer_balance : *engine_->peers_balance_) {
        peer_balance.second = 1;
    }
    engine_->SetNode();

    engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
    engine_->is_proposer_ = true;
    engine_->Start();

    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    bytes proposal = asBytes("payload");
    engine_->Propose(proposal, false, 1111, &prom);
    auto future = prom.get_future();
    EXPECT_EQ(future.get(),
              (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS, 1)));
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 1);
    EXPECT_TRUE(engine_->propose_state_.is_proposed_.load());
    EXPECT_EQ(engine_->propose_state_.last_failed_seq_.load(), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    engine_->Pass();
    engine_->PrintDiagnostics();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    for (int i = 1; i <= 3; ++i) {
        std::vector<ssz::ByteVector<32>> tmp_endorsed;
        auto pass_msg = CreatePassMessage(1, std::move(tmp_endorsed));
        engine_->OnConsensusMessage(peers_[i],
                                    std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        engine_->PrintDiagnostics();
    }
    while (engine_->last_consensus_seq_.load() != 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(engine_->last_consensus_seq_.load(), 1);
    EXPECT_FALSE(engine_->propose_state_.is_proposed_.load());
    EXPECT_EQ(engine_->propose_state_.propose_seq_.load(), 2);
    EXPECT_EQ(engine_->propose_state_.last_failed_seq_.load(), 1);
    engine_->PrintDiagnostics();
}

TEST_F(MyTumblerEngineTest, InactiveProposerTest) {
    engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
    auto new_check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
        engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
        return true;
    };

    engine_->check_validity_ = new_check_validity;

    for (auto& peer_balance : *engine_->peers_balance_) {
        peer_balance.second = 1;
    }
    engine_->SetNode();

    engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
    engine_->next_proposers_[6] = {peers_[0], peers_[1]};
    engine_->next_proposers_[7] = {peers_[2], peers_[3]};
    engine_->is_proposer_ = true;
    engine_->Start();

    // 1. 4 proposers: ABCD, complete 2 rounds of consensus normally, inactive set is empty
    // 2. Complete 3 rounds of consensus with ABC, received enough Pass to finish consensus, inactive set = {D}
    // 3. Still complete ABC's consensus, since inactive set is {D}, can send Pass. Continue consensus
    // 4. Received D's val message, remove from inactive set, complete ABD's consensus, cannot Pass, need to wait for C's consensus.
    //    C's val not received, received 3 pass messages, consensus completes directly, update inactive set to {C}
    // 5. Receive ABD's val messages and C's skip message, C is removed from inactive set
    for (Seq seq = 1; seq <= 2; ++seq) {
        bytes payload = asBytes("payload_" + std::to_string(seq));

        // simulate all 4 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : peers_) {
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is empty
        EXPECT_TRUE(engine_->inactive_proposers_.empty());
    }
    {
        // only the first three nodes propose and complete
        Seq seq = 3;
        bytes payload = asBytes("payload_" + std::to_string(seq));
        // simulate 3 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        for (const auto& proposer : peers_) {
            if (proposer == peers_[3]) {
                continue;
            }
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : peers_) {
            if (proposer == peers_[3]) {
                continue;
            }
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            if (proposer == peers_[3]) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is peers_[3]
        EXPECT_EQ(engine_->inactive_proposers_.size(), 1);
        EXPECT_EQ(engine_->inactive_proposers_.count(peers_[3]), 1);
    }
    {
        // still only the first three nodes propose and complete, no waiting needed this time
        Seq seq = 4;
        bytes payload = asBytes("payload_" + std::to_string(seq));
        // simulate 3 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        for (const auto& proposer : peers_) {
            if (proposer == peers_[3]) {
                continue;
            }
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : peers_) {
            if (proposer == peers_[3]) {
                continue;
            }
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            if (proposer == peers_[3]) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is peers_[3]
        EXPECT_EQ(engine_->inactive_proposers_.size(), 1);
        EXPECT_EQ(engine_->inactive_proposers_.count(peers_[3]), 1);
    }
    {
        // ABD propose and complete, need to wait for C this time, D removed from inactive
        Seq seq = 5;
        bytes payload = asBytes("payload_" + std::to_string(seq));
        // simulate 3 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        for (const auto& proposer : peers_) {
            if (proposer == peers_[2]) {
                continue;
            }
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : peers_) {
            if (proposer == peers_[2]) {
                continue;
            }
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            if (proposer == peers_[2]) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is peers_[2]
        EXPECT_EQ(engine_->inactive_proposers_.size(), 1);
        EXPECT_EQ(engine_->inactive_proposers_.count(peers_[2]), 1);
    }
    {
        // proposers changed to AB
        Seq seq = 6;
        bytes payload = asBytes("payload_" + std::to_string(seq));
        // simulate 3 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        auto current_proposers = engine_->current_proposers_;
        for (const auto& proposer : current_proposers) {
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : current_proposers) {
            if (proposer == peers_[2]) {
                continue;
            }
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is peers_[2]
        EXPECT_EQ(engine_->inactive_proposers_.size(), 1);
        EXPECT_EQ(engine_->inactive_proposers_.count(peers_[2]), 1);
    }
    {
        // proposers changed to CD, C remains inactive
        Seq seq = 7;
        bytes payload = asBytes("payload_" + std::to_string(seq));
        // simulate 3 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        auto current_proposers = engine_->current_proposers_;
        for (const auto& proposer : current_proposers) {
            if (proposer == peers_[2]) {
                continue;
            }
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : current_proposers) {
            if (proposer == peers_[2]) {
                continue;
            }
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is peers_[2]
        EXPECT_EQ(engine_->inactive_proposers_.size(), 1);
        EXPECT_EQ(engine_->inactive_proposers_.count(peers_[2]), 1);
    }
    {
        // ABD propose and complete, C skips, C removed from inactive
        Seq seq = 8;
        auto current_proposers = engine_->current_proposers_;
        bytes payload = asBytes("payload_" + std::to_string(seq));
        // simulate 3 proposers sending val messages
        std::map<NodeId, Digest> hashes;
        for (const auto& proposer : current_proposers) {
            if (proposer == peers_[2]) {
                auto skip_msg = CreateSkipMessage(seq, proposer, Signature());
                Signature tmp_sig = MockSignFunc(2, CalculateSkipDigest(skip_msg));

                skip_msg->signature = tmp_sig;
                engine_->OnConsensusMessage(
                    proposer,
                    std::make_shared<ssz_types::ConsensusMessage>(skip_msg));
                continue;
            }
            if (proposer == engine_->my_id_) {
                std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
                auto ts = NOWTIME;
                auto val_msg = CreateValMessage(seq, proposer, ts, payload, engine_->epoch_number_);
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                auto future = prom.get_future();
                engine_->Propose(payload, false, ts, &prom);
                auto state = future.get();
                EXPECT_EQ(
                    state,
                    (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS,
                                                           seq)));
            } else {
                auto val_msg =
                    CreateValMessage(seq, proposer, NOWTIME, payload, engine_->epoch_number_);
                val_msg->signature =
                    MockSignFunc(0,
                                 CalculateBvalDigestForValMessage(val_msg, engine_->spec_version_));
                hashes[proposer] = Digest(val_msg->hash.Acquire());
                engine_->OnConsensusMessage(proposer,
                                            std::make_shared<ssz_types::ConsensusMessage>(val_msg));
            }
        }

        // simulate BA completion
        for (const auto& proposer : current_proposers) {
            if (proposer == peers_[2]) {
                continue;
            }
            engine_->MyBACompleteCallBack(seq, proposer, hashes[proposer]);
        }

        // simulate receiving pass messages from others
        auto pass_msg = CreatePassMessage(seq, std::vector<ssz::ByteVector<32>>());
        for (const auto& proposer : peers_) {
            if (proposer == engine_->my_id_) {
                continue;
            }
            engine_->OnConsensusMessage(proposer,
                                        std::make_shared<ssz_types::ConsensusMessage>(pass_msg));
        }

        // wait for consensus to complete
        while (engine_->last_consensus_seq_.load() != seq) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        EXPECT_EQ(engine_->last_consensus_seq_.load(), seq);
        // verify inactive set is empty
        EXPECT_EQ(engine_->inactive_proposers_.size(), 0);
    }
}

TEST_F(MyTumblerEngineTest, SuperNodeTest) {
    // engine_->spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;

    auto new_check_validity = [&](Seq seq,
                                  const NodeId& id,
                                  const std::pair<uint32_t, uint32_t> index_epoch,
                                  std::shared_ptr<ABuffer> buffer) {
        engine_->DoOnCheckSanityDone(seq, id, index_epoch, std::nullopt);
        return true;
    };

    // engine_->check_validity_ = new_check_validity;

    // (*engine_->peers_balance_)[peers_[0]] = 1000;
    // engine_->SetNode();

    // engine_->current_proposers_ = {peers_[0], peers_[1], peers_[2], peers_[3]};
    // engine_->next_proposers_[6] = {peers_[0], peers_[1]};
    // engine_->next_proposers_[7] = {peers_[2], peers_[3]};
    // engine_->is_proposer_ = true;
    (*peer_balance_map_)[peers_[0]] = 1000;
    std::map<Seq, std::set<NodeId>> proposers;
    proposers[1] = {peers_[0], peers_[1], peers_[2], peers_[3]};
    engine_->Configure(
        CONSENSUS_VERSION_PIPELINE_PROPOSE,  // spec_version
        7,  // epoch_number
        my_id_,
        peer_pubkey_map_,
        peer_balance_map_,
        cc_,
        0,
        0,
        0,
        false,
        proposers,
        CreateWALStorage("log/fixture"),
        [this](const consensus_spec::NodeId& node, const consensus_spec::MessagePtr msg) {
            // send_f
            return;
        },
        [this](const consensus_spec::MessagePtr msg) {
            // broadcast_f
            return;
        },
        new_check_validity,
        [this](Seq seq,
               uint64_t ts,
               Digest digest,
               std::vector<ssz_types::VoteStatus>& vote_status) {
            return std::make_pair(
                false,
                std::map<consensus_spec::wal::EntryType, std::vector<bytes>>());
        },
        [](std::vector<BlockProofBuffer> stable_proofs,
           std::vector<BlockProofBuffer> checkpoints) {
            return;
        });
    engine_->Start();
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom;
    Seq seq = engine_->GetCurrSeq();
    auto ts = NOWTIME;
    NodeId proposer = peers_[0];
    bytes payload = asBytes("payload_" + std::to_string(seq));
    auto future = prom.get_future();
    engine_->Propose(payload, false, ts, &prom);
    auto state = future.get();
    EXPECT_EQ(state,
              (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS, seq)));
    
    std::pair<MyTumblerProposeState, Seq> ret;
    do {
        ret = engine_->CanPropose();
    } while (ret.first != MyTumblerProposeState::PROPOSE_READY);
    
    ++seq;
    payload = asBytes("payload_" + std::to_string(seq));
    std::promise<std::pair<MyTumblerProposeState, Seq>> prom2;
    auto future2 = prom2.get_future();
    engine_->Propose(payload, false, ts, &prom2);
    auto state2 = future2.get();
    EXPECT_EQ(state2,
              (std::pair<MyTumblerProposeState, Seq>(MyTumblerProposeState::PROPOSE_SUCCESS, seq)));
    while(engine_->last_consensus_seq_.load() != 2);
}

}  // namespace consensus_spec
