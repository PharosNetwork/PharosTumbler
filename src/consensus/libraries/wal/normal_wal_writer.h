// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <fstream>
#include <memory>
#include <unordered_map>

#include "consensus/libraries/wal/wal_types.h"

namespace consensus_spec {
namespace wal {

class NormalWalWriter {
  public:
    NormalWalWriter(const std::string& segment_file_name, const uint32_t expected_log_num);
    ~NormalWalWriter();

  public:
    void Append(uint64_t segment_id,
                EntryType type,
                const char* entry,
                size_t size,
                bool check_crc);
    void Sync();
    void Close();

  private:
    std::unique_ptr<std::ofstream> segment_file_;
    uint32_t log_number_{0};
};

}  // namespace wal
}  // namespace consensus_spec
