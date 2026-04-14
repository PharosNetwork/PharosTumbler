// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "consensus/common/consensus_common.h"
#include "consensus/libraries/common/type_define.h"
#include "consensus/libraries/common/const_define.h"
#include "consensus/schema/consensus.h"

namespace consensus_spec {

enum ConsensusMessageType : uint8_t {
    ValMessage = 0,
    BvalMessage,
    AuxMessage,
    PromMessage,
    PromCertificateMessage_DEPRECATED,
    PassMessage = 5,
    SkipMessage,
    TermMessage_DEPRECATED,
    DKGFinalStatusMessage_DEPRECATED,
    AggregatedBvalMessage,
    AggregatedMainVoteMessage = 10,
    ValReceiptMessage,
    BvalReceiptMessage,
    AuxReceiptMessage,
    PromReceiptMessage,
    PromCertificateReceiptMessage_DEPRECATED = 15,
    PassReceiptMessage,
    SkipReceiptMessage,
    TermReceiptMessage_DEPRECATED,
    DKGFinalStatusReceiptMessage_DEPRECATED,
    AggregatedBvalReceiptMessage = 20,
    AggregatedMainVoteReceiptMessage,
    RequestProposalMessage,
    ResponseProposalMessage,
    DKGAlreadyFinishMessage_DEPRECATED,
    ForwardSkipMessage = 25,
    ForwardSkipReceiptMessage,
};

using MessagePtr = std::shared_ptr<ssz_types::ConsensusMessage>;
using ValMessagePtr = std::shared_ptr<ssz_types::ValMessage>;
using ValReceiptMessagePtr = std::shared_ptr<ssz_types::ValReceiptMessage>;
using BvalMessagePtr = std::shared_ptr<ssz_types::BvalMessage>;
using BvalReceiptMessagePtr = std::shared_ptr<ssz_types::BvalReceiptMessage>;
using AuxMessagePtr = std::shared_ptr<ssz_types::AuxMessage>;
using AuxReceiptMessagePtr = std::shared_ptr<ssz_types::AuxReceiptMessage>;
using PromMessagePtr = std::shared_ptr<ssz_types::PromMessage>;
using PromReceiptMessagePtr = std::shared_ptr<ssz_types::PromReceiptMessage>;
using PassMessagePtr = std::shared_ptr<ssz_types::PassMessage>;
using PassReceiptMessagePtr = std::shared_ptr<ssz_types::PassReceiptMessage>;
using SkipMessagePtr = std::shared_ptr<ssz_types::SkipMessage>;
using SkipReceiptMessagePtr = std::shared_ptr<ssz_types::SkipReceiptMessage>;
using ForwardSkipMessagePtr = std::shared_ptr<ssz_types::ForwardSkipMessage>;
using ForwardSkipReceiptMessagePtr = std::shared_ptr<ssz_types::ForwardSkipReceiptMessage>;
using RequestProposalMessagePtr = std::shared_ptr<ssz_types::RequestProposalMessage>;
using ResponseProposalMessagePtr = std::shared_ptr<ssz_types::ResponseProposalMessage>;
using AggregatedBvalMessagePtr = std::shared_ptr<ssz_types::AggregatedBvalMessage>;
using AggregatedBvalReceiptMessagePtr = std::shared_ptr<ssz_types::AggregatedBvalReceiptMessage>;
using AggregatedMainVoteMessagePtr = std::shared_ptr<ssz_types::AggregatedMainVoteMessage>;
using AggregatedMainVoteReceiptMessagePtr =
    std::shared_ptr<ssz_types::AggregatedMainVoteReceiptMessage>;

ValMessagePtr CreateValMessage(const Seq seq,
                               const NodeId& proposer_id,
                               const Timestamp timestamp,
                               bytes payload,
                               const uint64_t epoch_number = 0);

Digest CalculateValMessageHash(const ValMessagePtr msg);

Digest CalculateBvalDigestForValMessage(const ValMessagePtr msg, const uint16_t version);

Digest CalculateBvalDigest(const Seq seq,
                           const NodeId& proposer_id,
                           const Round round,
                           const Digest& hash,
                           const uint64_t epoch_number = 0,
                           const uint16_t spec_version = 0);

ValReceiptMessagePtr CreateValReceiptMessage(const Seq seq,
                                             const NodeId& proposer_id,
                                             const uint64_t epoch_number = 0);

BvalMessagePtr CreateBvalMessage(const Seq seq,
                                 const NodeId& proposer_id,
                                 Round round,
                                 const Digest& hash,
                                 const bool val_exist = false,
                                 const uint64_t epoch_number = 0);

Digest CalculateBvalDigest(const BvalMessagePtr msg, const uint16_t spec_version = 0);

BvalReceiptMessagePtr CreateBvalReceiptMessage(const Seq seq,
                                               const NodeId& proposer_id,
                                               const Round round,
                                               const Digest& hash,
                                               const uint64_t epoch_number = 0);

AggregatedBvalMessagePtr CreateAggregatedBvalMessage(const Seq seq,
                                                     const NodeId& proposer_id,
                                                     const Round round,
                                                     const Digest& hash,
                                                     ssz_types::AggregateSignature aggr_signature);

AggregatedBvalReceiptMessagePtr CreateAggregatedBvalReceiptMessage(const Seq seq,
                                                                   const NodeId& proposer_id,
                                                                   const Round round);

AuxMessagePtr CreateAuxMessage(const Seq seq,
                               const NodeId& proposer_id,
                               const Round round,
                               const Digest& hash,
                               ssz_types::AggregateSignature aggr_signature);

Digest CalculateAuxDigest(const AuxMessagePtr msg);

AuxReceiptMessagePtr CreateAuxReceiptMessage(const Seq seq,
                                             const NodeId& proposer_id,
                                             const Round round);

PromMessagePtr CreatePromMessage(const Seq seq,
                                 const NodeId& proposer_id,
                                 const Round round,
                                 const Digest& hash,
                                 ssz_types::AggregateSignature aggr_signature);

Digest CalculatePromDigest(const PromMessagePtr msg);

PromReceiptMessagePtr CreatePromReceiptMessage(const Seq seq,
                                               const NodeId& proposer_id,
                                               const Round round);

AggregatedMainVoteMessagePtr CreateAggregatedMainVoteMessage(
    const Seq seq,
    const NodeId& proposer_id,
    const Round round,
    std::vector<ssz_types::AggrAuxInfo> aggr_aux_infos,
    std::vector<ssz_types::AggrPromInfo> aggr_prom_infos);

Digest CalculatePromDigestForAggregatedMainVoteMessage(const AggregatedMainVoteMessagePtr msg,
                                                       const Digest& hash);

Digest CalculateAuxDigestForAggregatedMainVoteMessage(const AggregatedMainVoteMessagePtr msg,
                                                      const Digest& hash);

AggregatedMainVoteReceiptMessagePtr
CreateAggregatedMainVoteReceiptMessage(const Seq seq, const NodeId& proposer_id, const Round round);

PassMessagePtr CreatePassMessage(const Seq seq, std::vector<ssz::ByteVector<32>>&& endorsed);

PassReceiptMessagePtr CreatePassReceiptMessage(const Seq seq);

SkipMessagePtr CreateSkipMessage(const Seq seq,
                                 const NodeId& proposer_id,
                                 const Signature& signature);

Digest CalculateSkipDigest(const SkipMessagePtr msg);

SkipReceiptMessagePtr CreateSkipReceiptMessage(const Seq seq, const NodeId& proposer_id);

ForwardSkipMessagePtr CreateForwardSkipMessage(const Seq seq,
                                               std::vector<ssz_types::Signature>&& skip_signatures);

ForwardSkipReceiptMessagePtr CreateForwardSkipReceiptMessage(const Seq seq);

RequestProposalMessagePtr CreateRequestProposalMessage(
    const Seq seq,
    std::vector<ssz_types::ProposalRequestKey> keys);

ResponseProposalMessagePtr CreateResponseProposalMessage(
    std::vector<std::shared_ptr<ssz_types::ValMessage>>& vals);

struct MessageQueueItem {
    NodeId sender_;
    MessagePtr msg_;
};

struct MessageGenericInfo {
    Seq seq_;
    NodeId proposer_id_;
    uint8_t msg_type_;
    Round round_;
    Digest hash_;

    MessageGenericInfo();
    MessageGenericInfo(const Seq seq,
                       const NodeId& proposer,
                       const uint32_t msg_type,
                       const Round round,
                       const Digest& hash);

    bool operator<(const MessageGenericInfo& other) const;
    bool operator==(const MessageGenericInfo& other) const;
};

struct SendQueueIndex {
    MessageGenericInfo msg_info_;
    NodeId receiver_id_;

    SendQueueIndex() = default;
    SendQueueIndex(const Seq seq,
                   const NodeId& proposer_id,
                   const NodeId& receiver_id,
                   const uint32_t msg_type,
                   const Round round,
                   const Digest& hash);
    explicit SendQueueIndex(const NodeId& receiver_id);

    bool operator<(const SendQueueIndex& other) const;
    bool operator==(const SendQueueIndex& other) const;
};

struct PersistentKey {
    MessageGenericInfo msg_info_;
    NodeId sender_id_;

    PersistentKey() = default;
    PersistentKey(const Seq seq,
                  const NodeId& proposer_id,
                  const NodeId& sender_id,
                  const uint32_t msg_type,
                  const Round round,
                  const Digest& hash);
    explicit PersistentKey(const NodeId& sender_id);

    bool operator<(const PersistentKey& other) const;
    bool operator==(const PersistentKey& other) const;
};

bool GetMessageInfo(const MessagePtr msg, MessageGenericInfo& msg_info);

}  // namespace consensus_spec
