// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/wal/wal.h"

#include <cstring>
#include <fstream>
#include <experimental/filesystem>

#include <cobre/libraries/common/macro.h>
#include <cobre/libraries/log/logging.h>
#include "consensus/libraries/wal/normal_wal_writer.h"

namespace consensus_spec {
namespace wal {

WalLocalStorage::WalLocalStorage(const Option& option) : option_(option) {
    if (option_.roll_interval_ < 10) {
        option_.roll_interval_ = 10;
    }
    if (!std::experimental::filesystem::exists(option_.path_)) {
        if (!std::experimental::filesystem::create_directory(option_.path_)) {
            COBRE_ABORT();
        }
    }
    // chmod 700 — owner-only access for WAL directory
    std::experimental::filesystem::permissions(
        option_.path_,
        std::experimental::filesystem::perms::owner_all);

    read_cache_.reset(new ReadCache());

    current_file_name_ = option_.path_ + "/" + CURRENT_FILE_NAME;
    old_file_name_ = option_.path_ + "/" + OLD_FILE_NAME;

    if (std::experimental::filesystem::exists(old_file_name_)) {
        auto [_, highest] = read_cache_->ReadLogFile(old_file_name_, option_.use_crc32_);
        old_high_ = highest;
    }
    if (std::experimental::filesystem::exists(current_file_name_)) {
        auto [next_log_num, highest] =
            read_cache_->ReadLogFile(current_file_name_, option_.use_crc32_);
        current_high_ = highest;
        writer_.reset(new NormalWalWriter(current_file_name_, next_log_num));
    } else {
        // no CURRENT
        writer_.reset(new NormalWalWriter(current_file_name_, 0));
    }
    is_dirty_ = false;
}

WalLocalStorage::~WalLocalStorage() {}

WAL_ERROR_CODE WalLocalStorage::Append(uint64_t segment_id,
                                       const std::map<EntryType, std::vector<bytes>>& entries) {
    if (entries.empty()) {
        return WAL_STATUS_OK;
    }
    UniqueGuard lock(mutex_);
    if (segment_id % option_.roll_interval_ == 0 && segment_id > current_high_
        && segment_id > old_high_ + 10) {
        Roll();
    }
    for (const auto& item : entries) {
        for (const auto& entry : item.second) {
            writer_->Append(segment_id,
                            item.first,
                            (const char*)entry.data(),
                            entry.size(),
                            option_.use_crc32_);
        }
    }
    if (option_.sync_) {
        writer_->Sync();
    }
    current_high_ = std::max(current_high_, segment_id);
    is_dirty_ = true;
    return WAL_STATUS_OK;
}

WAL_ERROR_CODE WalLocalStorage::GetEntries(uint64_t segment_id,
                                           EntryType type,
                                           std::vector<std::string>& entries) {
    UniqueGuard lock(mutex_);
    if (is_dirty_) {
        ReloadReader();
    }
    if (read_cache_ == nullptr) {
        return WAL_STATUS_NO_DATA;
    }
    return read_cache_->GetEntries(type, segment_id, entries);
}

std::pair<uint64_t, std::string> WalLocalStorage::GetLatestRawInfo() {
    UniqueGuard lock(mutex_);
    if (is_dirty_) {
        ReloadReader();
    }
    if (read_cache_ == nullptr) {
        return {0, 0};
    }
    return read_cache_->GetLatestRawInfo();
}

void WalLocalStorage::ReloadReader() {
    read_cache_.reset(new ReadCache());
    if (std::experimental::filesystem::exists(old_file_name_)) {
        read_cache_->ReadLogFile(old_file_name_, option_.use_crc32_);
    }
    if (std::experimental::filesystem::exists(current_file_name_)) {
        read_cache_->ReadLogFile(current_file_name_, option_.use_crc32_);
    }
    is_dirty_ = false;
}

void WalLocalStorage::Roll() {
    read_cache_->ClearEntries();  // now we do not need to read it anymore
    writer_.reset();
    std::experimental::filesystem::remove(old_file_name_);
    std::experimental::filesystem::rename(current_file_name_, old_file_name_);
    writer_.reset(new NormalWalWriter(current_file_name_, 0));
    old_high_ = current_high_;
}

}  // namespace wal
}  // namespace consensus_spec
