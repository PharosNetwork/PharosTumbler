// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <utility>
#include <chrono>
#include <map>
#include <functional>
#include <set>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <cobre/libraries/common/macro.h>
#include <cobre/libraries/log/logging.h>
#include "consensus/libraries/common/conversion.h"
#include "consensus/libraries/common/a_buffer.h"
#include "consensus/libraries/common/type_define.h"
#include "consensus/common/metrics.h"
#include "consensus/libraries/utils/time_utils.h"

namespace consensus_spec {

using IdToPublicKeyMap = std::map<NodeId, PublicKey>;
using IdToBalanceMap = std::map<NodeId, uint64_t>;
using IdProofPair = std::pair<NodeId, bytes>;
using SignFunc = std::function<void(const std::string& data, std::string& sig)>;
using VerifyFunc =
    std::function<bool(const std::string& data, const std::string& sig, const PublicKey& pub_key)>;
using DigestFunc = std::function<std::string(const std::string& data)>;

using AggregateSignFunc = std::function<void(const std::string& data, std::string& sig)>;
using AggregateVerifyFunc = std::function<bool(const std::string& sig,
                                               const std::string& node_set,
                                               const std::string& data,
                                               const size_t threshold)>;
using AggregateSignatureFunc = std::function<bool(std::string& aggr_sign, const std::string& sig)>;
using OptimisticAggrVerifyFunc = std::function<bool(const std::map<std::string, std::string>& sigs,
                                                    const std::string& data,
                                                    std::vector<std::string>& good_nodes,
                                                    std::vector<std::string>& bad_nodes,
                                                    std::string& output)>;

using GenerateBitMapFunc = std::function<std::string(const std::set<NodeId>& node_set)>;

using ExtractNodesFunc = std::function<std::vector<NodeId>(const std::string& bitmap)>;

class Digester {
  public:
    static std::string Digest(const std::string& data) {
        COBRE_ASSERT(digest_);
        return digest_(data);
    }

    static DigestFunc digest_;
};

class ECCSigner {
  public:
    ECCSigner() = default;

    ECCSigner(SignFunc sign_f, VerifyFunc verify_f);

  public:
    SignFunc sign_{nullptr};
    VerifyFunc verify_{nullptr};
};

class BLSAggregator {
  public:
    BLSAggregator() = default;

    BLSAggregator(AggregateSignFunc aggr_sign_f,
                  AggregateVerifyFunc aggr_verify_f,
                  AggregateSignatureFunc aggr_signature_f,
                  OptimisticAggrVerifyFunc opt_aggr_verify_f,
                  GenerateBitMapFunc gen_bitmap_f,
                  ExtractNodesFunc extract_nodes_f);

  public:
    AggregateSignFunc aggr_sign_{nullptr};
    AggregateVerifyFunc aggr_verify_{nullptr};
    AggregateSignatureFunc aggr_signature_{nullptr};
    OptimisticAggrVerifyFunc opt_aggr_verify_{nullptr};
    GenerateBitMapFunc gen_bitmap_{nullptr};
    ExtractNodesFunc extract_nodes_{nullptr};
};

class CryptoHelper {
  public:
    CryptoHelper() = default;

    void InitCryptoFunc(DigestFunc digest_f,
                        std::shared_ptr<ECCSigner> ecc_signer,
                        std::shared_ptr<BLSAggregator> aggregator);

    bool Sign(const std::string& data, std::string& sig);
    bool VerifySignature(const std::string& data,
                         const std::string& sig,
                         const NodeId& from,
                         std::shared_ptr<IdToPublicKeyMap> peers);

    bool AggregateSign(const std::string& data, std::string& sig);
    bool AggregateVerify(const std::string& sig,
                         const std::string& node_set,
                         const std::string& data,
                         const size_t threshold);
    bool AggregateSignature(std::string& aggr_sig, const std::string& sig);
    bool OptimisticAggrVerify(const std::map<std::string, std::string>& sigs,
                              const std::string& data,
                              std::vector<std::string>& good_nodes,
                              std::vector<std::string>& bad_nodes,
                              std::string& output);
    bool GenerateBitMap(const std::set<NodeId>& node_set, std::string& bitmap);
    bool ExtractNodes(const std::string& bitmap, std::vector<NodeId>& node_set);

    std::shared_ptr<ECCSigner> ecc_signer_;
    std::shared_ptr<BLSAggregator> aggregator_;
};

struct MyTumblerConfig {
    MyTumblerConfig() = default;

    MyTumblerConfig(const uint8_t consensus_window,
                    const size_t threads,
                    const size_t batch_size,
                    bool enable_pace_keeping,
                    const uint32_t pace_keeping_interval,
                    const uint64_t resend_timeout,
                    const uint64_t resend_interval,
                    const uint64_t broadcast_proof_interval,
                    bool enable_scalable_mytumbler,
                    const uint32_t proposer_shuffle_window,
                    const bool need_persist_message)
            : consensus_window_(consensus_window),
              threads_(threads),
              batch_size_(batch_size),
              enable_pace_keeping_(enable_pace_keeping),
              pace_keeping_interval_(pace_keeping_interval),
              reliable_channel_resend_timeout_(resend_timeout),
              reliable_channel_resend_interval_(resend_interval),
              broadcast_proof_interval_(broadcast_proof_interval),
              enable_scalable_mytumbler_(enable_scalable_mytumbler),
              proposer_shuffle_window_(proposer_shuffle_window),
              need_persist_message_(need_persist_message) {}

    uint8_t consensus_window_{1};
    size_t threads_{0};
    size_t batch_size_{0};
    bool enable_pace_keeping_{false};
    uint32_t pace_keeping_interval_;
    uint64_t reliable_channel_resend_timeout_{20000};
    uint64_t reliable_channel_resend_interval_{5000};
    uint64_t broadcast_proof_interval_{100};
    bool enable_scalable_mytumbler_{false};
    // uint32_t max_proposers_per_slot_{21};
    uint32_t proposer_shuffle_window_{2};
    bool need_persist_message_{true};
};

struct MyTumblerDiagnosticInfo {
    std::set<NodeId> curr_success_ba_;
    std::set<NodeId> curr_zero_ba_;
    std::set<NodeId> curr_skip_;
    std::map<NodeId, std::vector<bytes>> curr_pass_;
    std::set<NodeId> missing_nodes_;
};

struct ProposalKey {
    ProposalKey(const Seq seq, const NodeId& proposer);

    bool operator<(const ProposalKey& other) const;
    bool operator==(const ProposalKey& other) const;

    Seq seq_;
    NodeId proposer_id_;
};

enum MyTumblerProposeState : uint8_t {
    PROPOSE_FAILED_WINDOW_FULL = 0,
    PROPOSE_FAILED_IN_CONSENSUS = 1,
    PROPOSE_FAILED_REFUSE = 2,
    PROPOSE_SUCCESS = 3,
    PROPOSE_READY = 4,
    PROPOSE_FAILED_EPOCH_END = 5,
    PROPOSE_FAILED_NOT_PROPOSER = 6,
    PROPOSE_FAILED_NOT_ALLOW_EMPTY = 7,
};

}  // namespace consensus_spec
