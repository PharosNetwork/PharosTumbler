// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/protocol/mytumbler_message_types.h"
#include "consensus/libraries/log/consensus_log.h"

namespace consensus_spec {
ValMessagePtr CreateValMessage(const Seq seq,
                               const NodeId& proposer_id,
                               const Timestamp timestamp,
                               bytes payload,
                               const uint64_t epoch_number) {
    auto msg = std::make_shared<ssz_types::ValMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->timestamp = timestamp;
    msg->payload = std::move(payload);
    msg->epoch_number = epoch_number;
    auto hash = CalculateValMessageHash(msg);
    msg->hash = hash;

    return msg;
}

// TODO(v13): format digest to avoid hash collision, requires compatibility upgrade
Digest CalculateValMessageHash(const ValMessagePtr msg) {
    return Digester::Digest(std::string(msg->proposer_id.Acquire()) + std::to_string(msg->timestamp)
                            + std::string(msg->payload.Acquire()));
}

Digest CalculateBvalDigestForValMessage(const ValMessagePtr msg, const uint16_t spec_version) {
    (void)spec_version;  // V7+ only: digest includes epoch_number
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::BvalMessage))
                            + std::to_string(msg->seq) + std::string(msg->proposer_id.Acquire())
                            + std::to_string(Round(0)) + std::string(msg->hash.Acquire())
                            + std::to_string(msg->epoch_number));
}

ValReceiptMessagePtr CreateValReceiptMessage(const Seq seq,
                                             const NodeId& proposer_id,
                                             const uint64_t epoch_number) {
    auto msg = std::make_shared<ssz_types::ValReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->epoch_number = epoch_number;

    return msg;
}

BvalMessagePtr CreateBvalMessage(const Seq seq,
                                 const NodeId& proposer_id,
                                 Round round,
                                 const Digest& hash,
                                 const bool val_exist,
                                 const uint64_t epoch_number) {
    auto msg = std::make_shared<ssz_types::BvalMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;
    msg->hash = hash;
    msg->val_exist = val_exist;
    msg->epoch_number = epoch_number;

    return msg;
}

Digest CalculateBvalDigest(const BvalMessagePtr msg, const uint16_t spec_version) {
    (void)spec_version;  // V7+ only: digest includes epoch_number
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::BvalMessage))
                            + std::to_string(msg->seq) + std::string(msg->proposer_id.Acquire())
                            + std::to_string(msg->round) + std::string(msg->hash.Acquire())
                            + std::to_string(msg->epoch_number));
}

Digest CalculateBvalDigest(const Seq seq,
                           const NodeId& proposer_id,
                           const Round round,
                           const Digest& hash,
                           const uint64_t epoch_number,
                           const uint16_t spec_version) {
    (void)spec_version;  // V7+ only: digest includes epoch_number
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::BvalMessage))
                            + std::to_string(seq) + proposer_id + std::to_string(round) + hash
                            + std::to_string(epoch_number));
}

BvalReceiptMessagePtr CreateBvalReceiptMessage(const Seq seq,
                                               const NodeId& proposer_id,
                                               const Round round,
                                               const Digest& hash,
                                               const uint64_t epoch_number) {
    auto msg = std::make_shared<ssz_types::BvalReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;
    msg->hash = hash;
    msg->epoch_number = epoch_number;

    return msg;
}

AggregatedBvalMessagePtr CreateAggregatedBvalMessage(const Seq seq,
                                                     const NodeId& proposer_id,
                                                     const Round round,
                                                     const Digest& hash,
                                                     ssz_types::AggregateSignature aggr_signature) {
    auto msg = std::make_shared<ssz_types::AggregatedBvalMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;
    msg->hash = hash;
    msg->proof = std::move(aggr_signature);

    return msg;
}

AggregatedBvalReceiptMessagePtr CreateAggregatedBvalReceiptMessage(const Seq seq,
                                                                   const NodeId& proposer_id,
                                                                   const Round round) {
    auto msg = std::make_shared<ssz_types::AggregatedBvalReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;

    return msg;
}

AuxMessagePtr CreateAuxMessage(const Seq seq,
                               const NodeId& proposer_id,
                               const Round round,
                               const Digest& hash,
                               ssz_types::AggregateSignature aggr_signature) {
    auto msg = std::make_shared<ssz_types::AuxMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;
    msg->hash = hash;
    msg->aux_proof = std::move(aggr_signature);

    return msg;
}

Digest CalculateAuxDigest(const AuxMessagePtr msg) {
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::AuxMessage))
                            + std::to_string(msg->seq) + std::string(msg->proposer_id.Acquire())
                            + std::to_string(msg->round) + std::string(msg->hash.Acquire()));
}

AuxReceiptMessagePtr CreateAuxReceiptMessage(const Seq seq,
                                             const NodeId& proposer_id,
                                             const Round round) {
    auto msg = std::make_shared<ssz_types::AuxReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;

    return msg;
}

PromMessagePtr CreatePromMessage(const Seq seq,
                                 const NodeId& proposer_id,
                                 const Round round,
                                 const Digest& hash,
                                 ssz_types::AggregateSignature aggr_signature) {
    auto msg = std::make_shared<ssz_types::PromMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;
    msg->hash = hash;
    msg->prom_proof = std::move(aggr_signature);

    return msg;
}

Digest CalculatePromDigest(const PromMessagePtr msg) {
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::PromMessage))
                            + std::to_string(msg->seq) + std::string(msg->proposer_id.Acquire())
                            + std::to_string(msg->round) + std::string(msg->hash.Acquire()));
}

PromReceiptMessagePtr CreatePromReceiptMessage(const Seq seq,
                                               const NodeId& proposer_id,
                                               const Round round) {
    auto msg = std::make_shared<ssz_types::PromReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;

    return msg;
}

AggregatedMainVoteMessagePtr CreateAggregatedMainVoteMessage(
    const Seq seq,
    const NodeId& proposer_id,
    const Round round,
    std::vector<ssz_types::AggrAuxInfo> aggr_aux_infos,
    std::vector<ssz_types::AggrPromInfo> aggr_prom_infos) {
    auto msg = std::make_shared<ssz_types::AggregatedMainVoteMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;
    for (auto& aggr_aux_info : aggr_aux_infos) {
        msg->aggr_aux_infos.PushBack(std::move(aggr_aux_info));
    }
    for (auto& aggr_prom_info : aggr_prom_infos) {
        msg->aggr_prom_infos.PushBack(std::move(aggr_prom_info));
    }

    return msg;
}

Digest CalculatePromDigestForAggregatedMainVoteMessage(const AggregatedMainVoteMessagePtr msg,
                                                       const Digest& hash) {
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::PromMessage))
                            + std::to_string(msg->seq) + std::string(msg->proposer_id.Acquire())
                            + std::to_string(msg->round) + hash);
}

Digest CalculateAuxDigestForAggregatedMainVoteMessage(const AggregatedMainVoteMessagePtr msg,
                                                      const Digest& hash) {
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::AuxMessage))
                            + std::to_string(msg->seq) + std::string(msg->proposer_id.Acquire())
                            + std::to_string(msg->round) + hash);
}

AggregatedMainVoteReceiptMessagePtr CreateAggregatedMainVoteReceiptMessage(
    const Seq seq,
    const NodeId& proposer_id,
    const Round round) {
    auto msg = std::make_shared<ssz_types::AggregatedMainVoteReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->round = round;

    return msg;
}

PassMessagePtr CreatePassMessage(const Seq seq, std::vector<ssz::ByteVector<32>>&& endorsed) {
    auto msg = std::make_shared<ssz_types::PassMessage>();
    msg->seq = seq;
    msg->endorsed = std::move(endorsed);

    return msg;
}

PassReceiptMessagePtr CreatePassReceiptMessage(const Seq seq) {
    auto msg = std::make_shared<ssz_types::PassReceiptMessage>();
    msg->seq = seq;

    return msg;
}

SkipMessagePtr CreateSkipMessage(const Seq seq,
                                 const NodeId& proposer_id,
                                 const Signature& signature) {
    auto msg = std::make_shared<ssz_types::SkipMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;
    msg->signature = signature;

    return msg;
}

Digest CalculateSkipDigest(const SkipMessagePtr msg) {
    return Digester::Digest(std::to_string(static_cast<int32_t>(ConsensusMessageType::SkipMessage))
                            + std::to_string(msg->seq));
}

SkipReceiptMessagePtr CreateSkipReceiptMessage(const Seq seq, const NodeId& proposer_id) {
    auto msg = std::make_shared<ssz_types::SkipReceiptMessage>();
    msg->seq = seq;
    msg->proposer_id = proposer_id;

    return msg;
}

ForwardSkipMessagePtr CreateForwardSkipMessage(
    const Seq seq,
    std::vector<ssz_types::Signature>&& skip_signatures) {
    auto msg = std::make_shared<ssz_types::ForwardSkipMessage>();
    msg->seq = seq;
    msg->skip_signatures = std::move(skip_signatures);

    return msg;
}

ForwardSkipReceiptMessagePtr CreateForwardSkipReceiptMessage(const Seq seq) {
    auto msg = std::make_shared<ssz_types::ForwardSkipReceiptMessage>();
    msg->seq = seq;

    return msg;
}

RequestProposalMessagePtr CreateRequestProposalMessage(
    const Seq seq,
    std::vector<ssz_types::ProposalRequestKey> keys) {
    auto msg = std::make_shared<ssz_types::RequestProposalMessage>();
    msg->seq = seq;
    msg->keys = std::move(keys);

    return msg;
}

ResponseProposalMessagePtr CreateResponseProposalMessage(
    std::vector<std::shared_ptr<ssz_types::ValMessage>>& vals) {
    auto msg = std::make_shared<ssz_types::ResponseProposalMessage>();
    msg->vals = std::move(vals);

    return msg;
}

MessageGenericInfo::MessageGenericInfo()
        : seq_(0), proposer_id_(NodeId(32, '0')), msg_type_(0), round_(0), hash_(Digest()) {}

MessageGenericInfo::MessageGenericInfo(const Seq seq,
                                       const NodeId& proposer,
                                       const uint32_t msg_type,
                                       const Round round,
                                       const Digest& hash)
        : seq_(seq), proposer_id_(proposer), msg_type_(msg_type), round_(round), hash_(hash) {}

bool MessageGenericInfo::operator<(const MessageGenericInfo& other) const {
    if (seq_ < other.seq_) {
        return true;

    } else if (seq_ == other.seq_) {
        if (msg_type_ < other.msg_type_) {
            return true;

        } else if (msg_type_ == other.msg_type_) {
            if (proposer_id_ < other.proposer_id_) {
                return true;

            } else if (proposer_id_ == other.proposer_id_) {
                if (round_ < other.round_) {
                    return true;

                } else if (round_ == other.round_) {
                    return hash_ < other.hash_;
                }
            }
        }
    }
    return false;
}

bool MessageGenericInfo::operator==(const MessageGenericInfo& other) const {
    return seq_ == other.seq_ && proposer_id_ == other.proposer_id_ && round_ == other.round_
           && msg_type_ == other.msg_type_ && hash_ == other.hash_;
}

SendQueueIndex::SendQueueIndex(const NodeId& receiver_id) : receiver_id_(receiver_id) {}

SendQueueIndex::SendQueueIndex(const Seq seq,
                               const NodeId& proposer_id,
                               const NodeId& receiver_id,
                               const uint32_t msg_type,
                               const Round round,
                               const Digest& hash)
        : msg_info_(seq, proposer_id, msg_type, round, hash), receiver_id_(receiver_id) {}

bool SendQueueIndex::operator<(const SendQueueIndex& other) const {
    if (msg_info_.seq_ < other.msg_info_.seq_) {
        return true;
    } else if (msg_info_.seq_ == other.msg_info_.seq_) {
        if (receiver_id_ < other.receiver_id_) {
            return true;
        }
        if (receiver_id_ > other.receiver_id_) {
            return false;
        }
        return msg_info_ < other.msg_info_;
    }
    return false;
}

bool SendQueueIndex::operator==(const SendQueueIndex& other) const {
    return receiver_id_ == other.receiver_id_ && msg_info_ == other.msg_info_;
}

PersistentKey::PersistentKey(const NodeId& sender_id) : sender_id_(sender_id) {}

PersistentKey::PersistentKey(const Seq seq,
                             const NodeId& proposer_id,
                             const NodeId& sender_id,
                             const uint32_t msg_type,
                             const Round round,
                             const Digest& hash)
        : msg_info_(seq, proposer_id, msg_type, round, hash), sender_id_(sender_id) {}

bool PersistentKey::operator<(const PersistentKey& other) const {
    if (msg_info_.seq_ < other.msg_info_.seq_) {
        return true;
    } else if (msg_info_.seq_ == other.msg_info_.seq_) {
        if (sender_id_ < other.sender_id_) {
            return true;
        }
        if (sender_id_ > other.sender_id_) {
            return false;
        }
        return msg_info_ < other.msg_info_;
    }
    return false;
}

bool PersistentKey::operator==(const PersistentKey& other) const {
    return sender_id_ == other.sender_id_ && msg_info_ == other.msg_info_;
}

bool GetMessageInfo(const MessagePtr msg, MessageGenericInfo& msg_info) {
    if (msg == nullptr) {
        return false;
    }

    msg_info.msg_type_ = msg->Index();
    bool is_ok = false;
    switch (msg->Index()) {
        case consensus_spec::ConsensusMessageType::ValMessage: {
            auto val_msg = msg->ValData();
            if (val_msg == nullptr) {
                break;
            }
            msg_info.seq_ = val_msg->seq;
            msg_info.proposer_id_ = val_msg->proposer_id.Acquire();
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::BvalMessage: {
            auto bval_msg = msg->BvalData();
            if (bval_msg == nullptr) {
                break;
            }
            msg_info.seq_ = bval_msg->seq;
            msg_info.proposer_id_ = bval_msg->proposer_id.Acquire();
            msg_info.round_ = bval_msg->round;
            msg_info.hash_ = bval_msg->hash.Acquire();
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::AggregatedBvalMessage: {
            auto aggregated_bval_msg = msg->AggregatedBvalData();
            if (aggregated_bval_msg == nullptr) {
                break;
            }
            msg_info.seq_ = aggregated_bval_msg->seq;
            msg_info.proposer_id_ = aggregated_bval_msg->proposer_id.Acquire();
            msg_info.round_ = aggregated_bval_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::PromMessage: {
            auto prom_msg = msg->PromData();
            if (prom_msg == nullptr) {
                break;
            }
            msg_info.seq_ = prom_msg->seq;
            msg_info.proposer_id_ = prom_msg->proposer_id.Acquire();
            msg_info.round_ = prom_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::AuxMessage: {
            auto aux_msg = msg->AuxData();
            if (aux_msg == nullptr) {
                break;
            }
            msg_info.seq_ = aux_msg->seq;
            msg_info.proposer_id_ = aux_msg->proposer_id.Acquire();
            msg_info.round_ = aux_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::AggregatedMainVoteMessage: {
            auto aggregated_mainvote_msg = msg->AggregatedMainVoteData();
            if (aggregated_mainvote_msg == nullptr) {
                break;
            }
            msg_info.seq_ = aggregated_mainvote_msg->seq;
            msg_info.proposer_id_ = aggregated_mainvote_msg->proposer_id.Acquire();
            msg_info.round_ = aggregated_mainvote_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::ValReceiptMessage: {
            auto val_receipt_msg = msg->ValReceiptData();
            if (val_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = val_receipt_msg->seq;
            msg_info.proposer_id_ = val_receipt_msg->proposer_id.Acquire();
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::BvalReceiptMessage: {
            auto bval_receipt_msg = msg->BvalReceiptData();
            if (bval_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = bval_receipt_msg->seq;
            msg_info.proposer_id_ = bval_receipt_msg->proposer_id.Acquire();
            msg_info.round_ = bval_receipt_msg->round;
            msg_info.hash_ = bval_receipt_msg->hash.Acquire();
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::AggregatedBvalReceiptMessage: {
            auto aggregated_bval_receipt_msg = msg->AggregatedBvalReceiptData();
            if (aggregated_bval_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = aggregated_bval_receipt_msg->seq;
            msg_info.proposer_id_ = aggregated_bval_receipt_msg->proposer_id.Acquire();
            msg_info.round_ = aggregated_bval_receipt_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::PromReceiptMessage: {
            auto prom_receipt_msg = msg->PromReceiptData();
            if (prom_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = prom_receipt_msg->seq;
            msg_info.proposer_id_ = prom_receipt_msg->proposer_id.Acquire();
            msg_info.round_ = prom_receipt_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::AuxReceiptMessage: {
            auto aux_receipt_msg = msg->AuxReceiptData();
            if (aux_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = aux_receipt_msg->seq;
            msg_info.proposer_id_ = aux_receipt_msg->proposer_id.Acquire();
            msg_info.round_ = aux_receipt_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::AggregatedMainVoteReceiptMessage: {
            auto aggregated_mainvote_receipt_msg = msg->AggregatedMainVoteReceiptData();
            if (aggregated_mainvote_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = aggregated_mainvote_receipt_msg->seq;
            msg_info.proposer_id_ = aggregated_mainvote_receipt_msg->proposer_id.Acquire();
            msg_info.round_ = aggregated_mainvote_receipt_msg->round;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::PassMessage: {
            auto pass_msg = msg->PassData();
            if (pass_msg == nullptr) {
                break;
            }
            msg_info.seq_ = pass_msg->seq;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::PassReceiptMessage: {
            auto pass_receipt_msg = msg->PassReceiptData();
            if (pass_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = pass_receipt_msg->seq;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::SkipMessage: {
            auto skip_msg = msg->SkipData();
            if (skip_msg == nullptr) {
                break;
            }
            msg_info.seq_ = skip_msg->seq;
            msg_info.proposer_id_ = skip_msg->proposer_id.Acquire();
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::SkipReceiptMessage: {
            auto skip_receipt_msg = msg->SkipReceiptData();
            if (skip_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = skip_receipt_msg->seq;
            msg_info.proposer_id_ = skip_receipt_msg->proposer_id.Acquire();
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::ForwardSkipMessage: {
            auto forward_skip_msg = msg->ForwardSkipData();
            if (forward_skip_msg == nullptr) {
                break;
            }
            msg_info.seq_ = forward_skip_msg->seq;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::ForwardSkipReceiptMessage: {
            auto forward_skip_receipt_msg = msg->ForwardSkipReceiptData();
            if (forward_skip_receipt_msg == nullptr) {
                break;
            }
            msg_info.seq_ = forward_skip_receipt_msg->seq;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::RequestProposalMessage: {
            auto request_proposal_msg = msg->RequestProposalData();
            if (request_proposal_msg == nullptr) {
                break;
            }
            msg_info.seq_ = request_proposal_msg->seq;
            is_ok = true;
            break;
        }
        case consensus_spec::ConsensusMessageType::ResponseProposalMessage: {
            auto response_proposal_msg = msg->ResponseProposalData();
            if (response_proposal_msg == nullptr || response_proposal_msg->vals.Size() == 0) {
                break;
            }
            msg_info.seq_ = response_proposal_msg->vals[0]->seq;
            is_ok = true;
            break;
        }
        default: {
            CS_LOG_ERROR("Invalid msg type");
            break;
        }
    }

    return is_ok;
}

}  // namespace consensus_spec
