// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <iostream>
#include <experimental/filesystem>

#include "consensus/protocol/mytumbler_engine_base.h"
#include "consensus/libraries/wal/byte_order_utils.h"
#include "test/protocol/mock_reliable_channel.h"

namespace consensus_spec {
struct MockBLSAggregator {
    std::map<NodeId, uint32_t> id_to_index_;
};

struct mytumbler_state_machine_fixture {
    static std::string serialize_map(const std::map<uint8_t, std::string>& m) {
        std::string output;
        uint32_t map_size = m.size();
        wal::byte_order_utils::Writeu32_be(map_size, output);
        for (const auto& pair : m) {
            output.push_back(pair.first);
            wal::byte_order_utils::Writeu32_be(pair.second.size(), output);
            output.append(pair.second);
        }
        return output;
    }

    static std::map<uint8_t, std::string> deserialize_map(const std::string& data) {
        uint32_t size = wal::byte_order_utils::Readu32_be(data);
        std::string remaining = data.substr(4);
        std::map<uint8_t, std::string> m;
        for (size_t i = 0; i < size; ++i) {
            uint8_t key = remaining[0];
            remaining = remaining.substr(1);
            uint32_t str_size = wal::byte_order_utils::Readu32_be(remaining);
            remaining = remaining.substr(4);
            m[key] = remaining.substr(0, str_size);
            remaining = remaining.substr(str_size);
        }
        return m;
    }

    static bytes asBytes(const std::string& data) {
        return bytes(data.begin(), data.end());
    }

    static std::string MockHashFunc(const std::string& data) {
        return data + "hash";
    }

    static std::string MockSignFunc(const uint8_t id, const std::string& data) {
        if (id % 2 == 0) {
            return data + "x" + std::to_string(id);
        } else {
            return data + "yy" + std::to_string(id);
        }
    }

    static bool MockVerifyFunc(const uint8_t id,
                               const std::string& data,
                               const std::string& signature) {
        if (id % 2 == 0) {
            return signature == data + "x" + std::to_string(id);
        } else {
            return signature == data + "yy" + std::to_string(id);
        }
    }

    static std::string MockAggregateSign(const uint8_t id, const std::string& data) {
        std::map<uint8_t, std::string> m;
        m[id] = MockSignFunc(id, data);
        return serialize_map(m);
    }

    static bool MockAggregateVerifyFunc(const std::string& aggr_sig,
                                        const std::string& bitmap,
                                        const std::string& data,
                                        const size_t threshold) {
        std::vector<uint8_t> node_set = MockExtractNodes(bitmap);
        std::map<uint8_t, std::string> m = deserialize_map(aggr_sig);
        if (m.size() < threshold) {
            return false;
        }
        for (const auto i : node_set) {
            if (m.count(i) == 0) {
                return false;
            } else if (m[i] != MockSignFunc(i, data)) {
                std::cout << m[i] << " not " << MockSignFunc(i, data) << std::endl;
                return false;
            }
        }
        return true;
    }

    static void MockAggregateSignature(std::string& target_sig, const std::string& sig) {
        std::map<uint8_t, std::string> m1 = deserialize_map(target_sig);
        std::map<uint8_t, std::string> m2 = deserialize_map(sig);
        m1.merge(m2);
        target_sig = serialize_map(m1);
    }

    static std::string MockGenerateBitmap(const std::set<uint8_t> ids) {
        std::string bitmap;
        for (const auto i : ids) {
            bitmap.push_back((char)i);
        }
        return bitmap;
    }

    static std::vector<uint8_t> MockExtractNodes(const std::string& bitmap) {
        std::vector<uint8_t> node_set;
        for (const char i : bitmap) {
            node_set.push_back((uint8_t)i);
        }
        return node_set;
    }

    static MyTumblerConfig GenerateMyTumblerConfig(size_t batch_size,
                                                   size_t threads,
                                                   bool enable_pace_keeping) {
        MyTumblerConfig cfg;
        cfg.batch_size_ = batch_size;
        cfg.consensus_window_ = 10;
        cfg.enable_pace_keeping_ = enable_pace_keeping;
        cfg.pace_keeping_interval_ = 10;
        cfg.reliable_channel_resend_timeout_ = 20000;
        cfg.reliable_channel_resend_interval_ = 5000;
        cfg.broadcast_proof_interval_ = 100;
        cfg.threads_ = threads;
        return cfg;
    }

    static std::shared_ptr<BLSAggregator> CreateAggregator(uint8_t my_id,
                                                           uint8_t node_size,
                                                           std::vector<NodeId> ids) {
        AggregateSignFunc aggr_sign_f = [my_id](const std::string& data, std::string& sig) {
            sig = MockAggregateSign(my_id, data);
        };
        AggregateVerifyFunc aggr_verify_f = [](const std::string& sig,
                                               const std::string& bitmap,
                                               const std::string& data,
                                               const size_t threshold) {
            return MockAggregateVerifyFunc(sig, bitmap, data, threshold);
        };
        AggregateSignatureFunc aggr_signature_f = [](std::string& aggr_sign,
                                                     const std::string& sig) {
            MockAggregateSignature(aggr_sign, sig);
            return true;
        };
        OptimisticAggrVerifyFunc opt_aggr_verify_f =
            [ids](const std::map<std::string, std::string>& sigs,
                  const std::string& data,
                  std::vector<std::string>& good_nodes,
                  std::vector<std::string>& bad_nodes,
                  std::string& output) -> bool {
            std::string aggregated_sig;
            for (const auto& bitmap_sig : sigs) {
                std::vector<uint8_t> cur_index = MockExtractNodes(bitmap_sig.first);
                if (MockAggregateVerifyFunc(bitmap_sig.second, bitmap_sig.first, data, 1)) {
                    good_nodes.push_back(ids[cur_index[0]]);
                    if (aggregated_sig.empty()) {
                        aggregated_sig = bitmap_sig.second;
                    } else {
                        MockAggregateSignature(aggregated_sig, bitmap_sig.second);
                    }
                } else {
                    bad_nodes.push_back(ids[cur_index[0]]);
                }
            }
            output = aggregated_sig;
            return !output.empty();
        };
        GenerateBitMapFunc gen_bitmap_f = [ids](const std::set<NodeId>& node_set) {
            std::set<uint8_t> included;
            for (const auto& id : node_set) {
                for (size_t i = 0; i < ids.size(); ++i) {
                    if (ids[i] == id) {
                        included.emplace(i);
                    }
                }
            }
            return MockGenerateBitmap(included);
        };
        ExtractNodesFunc extract_nodes_f = [ids](const std::string& bitmap) {
            std::vector<uint8_t> included = MockExtractNodes(bitmap);
            std::vector<NodeId> result;
            for (const auto& i : included) {
                result.push_back(ids[i]);
            }
            return result;
        };
        return std::make_shared<BLSAggregator>(aggr_sign_f,
                                               aggr_verify_f,
                                               aggr_signature_f,
                                               opt_aggr_verify_f,
                                               gen_bitmap_f,
                                               extract_nodes_f);
    }

    static std::shared_ptr<wal::WalLocalStorage> CreateWALStorage(const std::string& db_path) {
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::make_shared<wal::WalLocalStorage>(option);
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);
        wal = std::make_shared<wal::WalLocalStorage>(option);
        return wal;
    }

    mytumbler_state_machine_fixture() {
        engine_ = std::make_shared<MyTumblerEngineBase>();
        spec_version_ = CONSENSUS_VERSION_PIPELINE_PROPOSE;
        peer_pubkey_map_ = CreatePeers();
        peer_balance_map_ = std::make_shared<IdToBalanceMap>();
        int init_balance = 1;
        for (const auto& peer : *peer_pubkey_map_) {
            (*peer_balance_map_)[peer.first] = init_balance;
        }
        for (const auto peer : *peer_pubkey_map_) {
            peers_.push_back(peer.first);
        }
        uint64_t epoch_number = 1;
        for (Seq seq = 1; seq <= 5; ++seq) {
            CreateProposalHash(seq, epoch_number);
        }
        epoch_number = 2;
        for (Seq seq = 5; seq < 10; ++seq) {
            CreateProposalHash(seq, epoch_number);
        }
        my_id_ = peer_pubkey_map_->begin()->first;
        std::map<Seq, std::set<NodeId>> next_proposers;
        std::set<NodeId> proposers;
        for (const auto& peer : peers_) {
            proposers.insert(peer);
        }
        next_proposers[0] = proposers;
        next_proposers[20] = proposers;
        aggregator_ = CreateAggregator(0, peers_.size(), peers_);
        cc_ = GenerateMyTumblerConfig(10, 4, false);
        auto ecc_signer = std::make_shared<ECCSigner>(
            [this](const std::string& data, std::string& sig) {
                sig = MockSignFunc(0, data);
            },
            [this](const std::string& data, const std::string& sig, const PublicKey& from) {
                size_t i = 0;
                for (auto itr = peer_pubkey_map_->begin(); itr != peer_pubkey_map_->end(); ++itr) {
                    if (itr->second == from) {
                        return MockVerifyFunc(i, data, sig);
                    }
                    ++i;
                }
                return false;
            });
        engine_->InitCryptoFunc(
            [this](const std::string& data) {
                return MockHashFunc(data);
            },
            ecc_signer,
            aggregator_);
        engine_->Configure(
            spec_version_,
            0,
            my_id_,
            peer_pubkey_map_,
            peer_balance_map_,
            cc_,
            0,
            0,
            0,
            false,
            next_proposers,
            CreateWALStorage("log/fixture"),
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
            });
    }

    std::shared_ptr<IdToPublicKeyMap> CreatePeers(size_t n = 7, size_t node_index = 0) {
        auto id_2_pks = std::make_shared<IdToPublicKeyMap>();
        for (size_t i = 0; i < n; ++i) {
            NodeId tmp_id = "00" + std::to_string(i) + std::string(29, '0');
            id_2_pks->emplace(tmp_id, std::to_string(i));
        }
        return id_2_pks;
    }

    void CreateProposalHash(Seq seq, uint64_t epoch_number) {
        for (auto node : peers_) {
            std::string mock_proposal = std::to_string(seq) + std::to_string(epoch_number) + node;
            proposal_hash_map_[seq][node] = Digest(mock_proposal);
        }
    }

  public:
    MyTumblerEngineBasePtr engine_;
    uint16_t spec_version_;
    NodeId my_id_;
    std::shared_ptr<IdToPublicKeyMap> peer_pubkey_map_;
    std::shared_ptr<IdToBalanceMap> peer_balance_map_;
    MyTumblerConfig cc_;
    std::vector<NodeId> peers_;
    std::map<Seq, std::map<NodeId, Digest>> proposal_hash_map_;
    std::shared_ptr<BLSAggregator> aggregator_;
};

using MockReliableChannelPtr = std::shared_ptr<MockReliableChannel>;
}  // namespace consensus_spec