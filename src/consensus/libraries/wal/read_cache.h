// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <unordered_map>
#include <map>
#include <vector>
#include <cstdint>

#include "consensus/libraries/wal/wal_types.h"

namespace consensus_spec {
namespace wal {

class ReadCache {
  public:
    ReadCache();
    ~ReadCache() = default;

  public:
    // returns the next expected log number to append, and the highest segment
    std::pair<uint32_t, uint64_t> ReadLogFile(const std::string& filename, bool check_crc);

    WAL_ERROR_CODE GetEntries(EntryType type,
                              uint64_t segment_id,
                              std::vector<std::string>& entries);
    std::pair<uint64_t, std::string> GetLatestRawInfo();
    void ClearEntries();

  private:
    std::unordered_map<EntryType, std::map<uint64_t, std::vector<std::string>>> entries_;
    std::pair<uint64_t, std::string> latest_raw_info_{0, ""};
    // std::pair<uint64_t, uint64_t> latest_raw_ts_{0, 0};
};

}  // namespace wal
}  // namespace consensus_spec
