// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>

namespace consensus_spec {

// for reliable channel
const std::string RC_RESEND_QUEUE_SIZE = "rc_resend_queue_size";
const std::string RC_RESEND_DURATION = "rc_resend_duration";
const std::string RC_PERSIST_QUEUE_SIZE = "rc_persist_queue_size";
const std::string RC_PERSIST_DURATION = "rc_persist_duration";
const std::string RC_BROADCAST_QUEUE_SIZE = "rc_broadcast_queue_size";
const std::string RC_SEND_QUEUE_SIZE = "rc_send_queue_size";
const std::string RC_SEND_DURATION = "rc_send_duration";
const std::string RC_RECEIPT_QUEUE_SIZE = "rc_receipt_queue_size";
const std::string RC_RECEIPT_DURATION = "rc_receipt_duration";
const std::string RC_GC_DURAION = "rc_gc_duration";

// for myba
const std::string MYBA_ZERO_COUNT = "myba_zero_count";
const std::string MYBA_DURATION = "myba_duration";

// for mytumbler
const std::string MYTUMBLER_VALIDATOR_NUMBER = "mytumbler_validator_number";
const std::string MYTUMBLER_TOTAL_BALANCE = "mytumbler_total_balance";
const std::string MYTUMBLER_VAL_SIZE = "mytumbler_val_size";
const std::string MYTUMBLER_VAL_DURATION = "mytumbler_val_duration";
const std::string MYTUMBLER_PASS_TIME = "mytumbler_pass_time";
const std::string MYTUMBLER_CONSENSUS_TIME = "mytumbler_consensus_time";
const std::string MYTUMBLER_IDLE_TIME = "mytumbler_idle_time";
const std::string MYTUMBLER_SKIP_COUNT = "mytumbler_skip_count";
const std::string MYTUMBLER_MY_SUCCESS_COUNT = "mytumbler_my_success_count";
const std::string MYTUMBLER_SUCCESS_PROPOSAL_COUNT = "mytumbler_success_proposal_count";
const std::string MYTUMBLER_CONTRIBUTED_PROPOSER_COUNT = "mytumbler_contributed_proposer_count";
const std::string MYTUMBLER_SYNC_BLOCK_GAUGE = "mytumbler_sync_block_gauge";
const std::string MYTUMBLER_WINDOW_INDEX = "mytumbler_window_index";
const std::string MYTUMBLER_WINDOW_FULL = "mytumbler_window_full";

// for scalable mytumbler
const std::string MYTUMBLER_AGGR_SIGN_DURATION = "mytumbler_aggr_sign_duration";
const std::string MYTUMBLER_AGGR_OPT_VERIFY_DURATION = "mytumbler_aggr_opt_verify_duration";
const std::string MYTUMBLER_AGGR_VERIFY_DURATION = "mytumbler_aggr_verify_duration";

// for storage IO
const std::string CONSENSUS_WAL_WRITE_COUNT = "consensus_wal_write_count";
const std::string CONSENSUS_WAL_WRITE_BYTES = "consensus_wal_write_bytes";
const std::string CONSENSUS_WAL_READ_COUNT = "consensus_wal_read_count";
const std::string CONSENSUS_WAL_READ_BYTES = "consensus_wal_read_bytes";
}  // namespace consensus_spec
