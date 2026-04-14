// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/common/consensus_common.h"

#include <cobre/libraries/log/logging.h>
#include <pamir/cetina/cetina.h>
#include "consensus/libraries/log/consensus_log.h"

namespace consensus_spec {

DigestFunc Digester::digest_ = nullptr;

ECCSigner::ECCSigner(SignFunc sign_f, VerifyFunc verify_f) {
    sign_ = sign_f;
    verify_ = verify_f;
}

BLSAggregator::BLSAggregator(AggregateSignFunc aggr_sign_f,
                             AggregateVerifyFunc aggr_verify_f,
                             AggregateSignatureFunc aggr_signature_f,
                             OptimisticAggrVerifyFunc opt_aggr_verify_f,
                             GenerateBitMapFunc gen_bitmap_f,
                             ExtractNodesFunc extract_nodes_f) {
    aggr_sign_ = aggr_sign_f;
    aggr_verify_ = aggr_verify_f;
    aggr_signature_ = aggr_signature_f;
    opt_aggr_verify_ = opt_aggr_verify_f;
    gen_bitmap_ = gen_bitmap_f;
    extract_nodes_ = extract_nodes_f;
}

void CryptoHelper::InitCryptoFunc(DigestFunc digest_f,
                                  std::shared_ptr<ECCSigner> ecc_signer,
                                  std::shared_ptr<BLSAggregator> aggregator) {
    COBRE_ASSERT(digest_f);
    COBRE_ASSERT(ecc_signer);
    COBRE_ASSERT(aggregator);
    Digester::digest_ = digest_f;

    ecc_signer_ = ecc_signer;
    aggregator_ = aggregator;
}

bool CryptoHelper::Sign(const std::string& data, std::string& sig) {
    COBRE_ASSERT(ecc_signer_);
    ecc_signer_->sign_(data, sig);
    return true;
}

bool CryptoHelper::VerifySignature(const std::string& data,
                                   const std::string& sig,
                                   const NodeId& from,
                                   std::shared_ptr<IdToPublicKeyMap> peers) {
    COBRE_ASSERT(ecc_signer_);
    COBRE_ASSERT(peers);
    auto itr = peers->find(from);
    if (itr == peers->end()) {
        CS_LOG_ERROR("Node {} doesn't exist", toHex(from).substr(0, 8));
        return false;
    }
    return ecc_signer_->verify_(data, sig, itr->second);
}

bool CryptoHelper::AggregateSign(const std::string& data, std::string& sig) {
    COBRE_ASSERT(aggregator_);
    auto start_time = time_utils::GetSteadyTimePoint();
    aggregator_->aggr_sign_(data, sig);
    CETINA_GAUGE_SET(MYTUMBLER_AGGR_SIGN_DURATION, time_utils::GetDuration(start_time));
    return true;
}

bool CryptoHelper::AggregateVerify(const std::string& sig,
                                   const std::string& node_set,
                                   const std::string& data,
                                   const size_t threshold) {
    COBRE_ASSERT(aggregator_);
    auto start_time = time_utils::GetSteadyTimePoint();
    auto ret = aggregator_->aggr_verify_(sig, node_set, data, threshold);
    CETINA_GAUGE_SET(MYTUMBLER_AGGR_VERIFY_DURATION, time_utils::GetDuration(start_time));
    return ret;
}

bool CryptoHelper::AggregateSignature(std::string& aggr_sig, const std::string& sig) {
    COBRE_ASSERT(aggregator_);
    return aggregator_->aggr_signature_(aggr_sig, sig);
}

bool CryptoHelper::OptimisticAggrVerify(const std::map<std::string, std::string>& sigs,
                                        const std::string& data,
                                        std::vector<std::string>& good_nodes,
                                        std::vector<std::string>& bad_nodes,
                                        std::string& output) {
    COBRE_ASSERT(aggregator_);
    auto start_time = time_utils::GetSteadyTimePoint();
    auto ret = aggregator_->opt_aggr_verify_(sigs, data, good_nodes, bad_nodes, output);
    CETINA_GAUGE_SET(MYTUMBLER_AGGR_OPT_VERIFY_DURATION, time_utils::GetDuration(start_time));
    return ret;
}

bool CryptoHelper::GenerateBitMap(const std::set<NodeId>& node_set, std::string& bitmap) {
    COBRE_ASSERT(aggregator_);
    bitmap = aggregator_->gen_bitmap_(node_set);
    return true;
}

bool CryptoHelper::ExtractNodes(const std::string& bitmap, std::vector<NodeId>& node_set) {
    COBRE_ASSERT(aggregator_);
    node_set = aggregator_->extract_nodes_(bitmap);
    return true;
}

ProposalKey::ProposalKey(const Seq seq, const NodeId& proposer)
        : seq_(seq), proposer_id_(proposer) {}

bool ProposalKey::operator<(const ProposalKey& other) const {
    if (seq_ < other.seq_) {
        return true;
    } else if (seq_ == other.seq_) {
        return proposer_id_ < other.proposer_id_;
    }
    return false;
}

bool ProposalKey::operator==(const ProposalKey& other) const {
    return seq_ == other.seq_ && proposer_id_ == other.proposer_id_;
}

}  // namespace consensus_spec
