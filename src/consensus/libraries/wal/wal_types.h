// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <cstdint>

namespace consensus_spec {
namespace wal {

// CRC(4) | log_num(4) | seq(8) | type(1) | data_size(4) | data_payload
const int ENTRY_HEAD_LENGTH = 21;

const std::string CURRENT_FILE_NAME = "CURRENT";
const std::string OLD_FILE_NAME = "OLD";

enum WAL_KEY_TYPE : uint8_t {
    RAW_BLOCK_KEY = 0,
};

enum WAL_ERROR_CODE : uint8_t {
    WAL_STATUS_OK = 0,
    WAL_STATUS_NO_DATA
};

enum EntryType : uint8_t {
    WAL_ENTRY_TYPE_ALL = 0,
    WAL_ENTRY_TYPE_MESSAGE,
    WAL_ENTRY_TYPE_CONSENSUS_RESULT,
    WAL_ENTRY_TYPE_TIMESTAMP,
};

}  // namespace wal
}  // namespace consensus_spec
