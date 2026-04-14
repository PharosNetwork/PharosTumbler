// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/wal/read_cache.h"

#include <inttypes.h>
#include <zlib.h>

#include <fstream>
#include <experimental/filesystem>

#include <pamir/cetina/cetina.h>
#include "consensus/common/metrics.h"
#include "consensus/libraries/log/consensus_log.h"
#include "consensus/libraries/wal/byte_order_utils.h"

namespace consensus_spec {
namespace wal {
ReadCache::ReadCache() : latest_raw_info_({0, ""}) {}

std::pair<uint32_t, uint64_t> ReadCache::ReadLogFile(const std::string& filename, bool check_crc) {
    std::ifstream in(filename, std::ios::in | std::ios::binary);
    in.exceptions(std::ios::badbit | std::ios::failbit);
    in.seekg(0, std::ios::end);
    auto length = in.tellg();
    in.seekg(0, std::ios::beg);
    char head[ENTRY_HEAD_LENGTH];
    uint32_t expected_log_number = 0;  // the next expected number
    uint64_t highest_segment = 0;
    while (in.tellg() < length) {
        auto position = in.tellg();
        if (in.tellg() + std::streampos(ENTRY_HEAD_LENGTH) > length) {
            std::experimental::filesystem::resize_file(filename, position);
            CS_LOG_ERROR("entry has incorrect data");
            in.close();
            return {expected_log_number, highest_segment};
        }
        CETINA_COUNTER_INC(CONSENSUS_WAL_READ_COUNT);
        CETINA_COUNTER_ADD(CONSENSUS_WAL_READ_BYTES, ENTRY_HEAD_LENGTH);
        in.read(head, ENTRY_HEAD_LENGTH);
        // CRC(4) | log_num(4) | seq(8) | type(1) | data_size(4) | data_payload
        uint32_t crc = byte_order_utils::Readu32_be(std::string_view(head, 4));
        uint32_t log_num = byte_order_utils::Readu32_be(std::string_view(&head[4], 4));
        if (expected_log_number != log_num) {
            CS_LOG_ERROR("unexpected log number: {} instead of {}", log_num, expected_log_number);
        }

        uint64_t segment_id = byte_order_utils::Readu64_be(std::string_view(&head[8], 8));
        EntryType type = EntryType(head[16]);
        uint32_t size = byte_order_utils::Readu32_be(std::string_view(&head[17], 4));
        // read payload
        if (in.tellg() + std::streampos(size) > length) {
            std::experimental::filesystem::resize_file(filename, position);
            CS_LOG_ERROR("entry has incorrect data");
            in.close();
            return {expected_log_number, highest_segment};
        }
        std::string data(size, '0');
        CETINA_COUNTER_INC(CONSENSUS_WAL_READ_COUNT);
        CETINA_COUNTER_ADD(CONSENSUS_WAL_READ_BYTES, size);
        in.read(data.data(), size);

        if (check_crc) {
            uint32_t real_crc = crc32(0L, Z_NULL, 0);
            real_crc = crc32(real_crc, (const Bytef*)(head + 4), ENTRY_HEAD_LENGTH - 4);
            real_crc = crc32(real_crc, (const Bytef*)data.data(), data.size());
            if (crc != real_crc) {
                std::experimental::filesystem::resize_file(filename, position);
                CS_LOG_ERROR("entry has incorrect crc");
                in.close();
                return {expected_log_number, highest_segment};
            }
        }

        if (type == EntryType::WAL_ENTRY_TYPE_TIMESTAMP) {
            // update raw_block and timestamp
            if (segment_id > latest_raw_info_.first) {
                latest_raw_info_ = {segment_id, data};
            }
        }
        entries_[type][segment_id].emplace_back(std::move(data));
        CS_LOG_INFO("read log number {}, type {}, sequence {}", log_num, type, segment_id);

        expected_log_number = log_num + 1;
        highest_segment = std::max(highest_segment, segment_id);
    }
    in.close();
    return {expected_log_number, highest_segment};
}

std::pair<uint64_t, std::string> ReadCache::GetLatestRawInfo() {
    return latest_raw_info_;
}

WAL_ERROR_CODE ReadCache::GetEntries(EntryType type,
                                     uint64_t segment_id,
                                     std::vector<std::string>& entries) {
    if (entries_.find(type) != entries_.end()
        && entries_[type].find(segment_id) != entries_[type].end()) {
        entries = entries_[type][segment_id];
        return WAL_STATUS_OK;
    }
    return WAL_STATUS_NO_DATA;
}

void ReadCache::ClearEntries() {
    entries_.clear();
}

}  // namespace wal
}  // namespace consensus_spec
