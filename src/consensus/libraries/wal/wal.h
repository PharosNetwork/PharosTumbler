// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <memory>
#include <vector>
#include <map>
#include <unordered_map>

#include "consensus/libraries/wal/normal_wal_writer.h"
#include "consensus/libraries/wal/read_cache.h"
#include "consensus/libraries/common/conversion.h"
#include "consensus/libraries/thread/lock.h"

namespace consensus_spec {
namespace wal {

struct Option {
    bool sync_{true};
    bool use_mmap_{false};
    bool use_crc32_{true};
    std::string path_;
    unsigned int roll_interval_{50};
};

class WalLocalStorage {
  public:
    explicit WalLocalStorage(const Option& option);
    ~WalLocalStorage();

  public:
    // append log entries
    WAL_ERROR_CODE Append(uint64_t segment_id,
                          const std::map<EntryType, std::vector<bytes>>& entries);
    // get all entries by specific type in segment_id
    WAL_ERROR_CODE GetEntries(uint64_t segment_id,
                              EntryType type,
                              std::vector<std::string>& entries);
    std::pair<uint64_t, std::string> GetLatestRawInfo();

  private:
    void Roll();
    void ReloadReader();

  private:
    Mutex mutex_;
    bool is_dirty_{false};
    Option option_;

    std::string current_file_name_;
    std::string old_file_name_;
    uint64_t old_high_{0};
    uint64_t current_high_{0};
    std::unique_ptr<NormalWalWriter> writer_{nullptr};
    std::unique_ptr<ReadCache> read_cache_{nullptr};
};

}  // namespace wal
}  // namespace consensus_spec
