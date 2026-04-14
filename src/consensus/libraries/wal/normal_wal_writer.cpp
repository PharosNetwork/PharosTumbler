// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/wal/normal_wal_writer.h"

#include <zlib.h>
#include <pamir/cetina/cetina.h>
#include <cobre/libraries/common/macro.h>
#include <cobre/libraries/log/logging.h>
#include "consensus/common/metrics.h"
#include "consensus/libraries/wal/byte_order_utils.h"

namespace consensus_spec {
namespace wal {

NormalWalWriter::NormalWalWriter(const std::string& segment_file_name,
                                 const uint32_t expected_log_num)
        : log_number_(expected_log_num) {
    segment_file_ =
        std::make_unique<std::ofstream>(segment_file_name,
                                        std::ios::out | std::ios::binary | std::ios::app);
    // segment_file_->exceptions(std::ios::failbit | std::ios::badbit);
    segment_file_->flush();
    if (segment_file_->fail()) {
        COBRE_ABORT();
    }
}

NormalWalWriter::~NormalWalWriter() {
    Close();
}

void NormalWalWriter::Append(uint64_t segment_id,
                             EntryType type,
                             const char* entry,
                             size_t size,
                             bool check_crc) {
    std::string to_write_buffer;
    to_write_buffer.reserve(ENTRY_HEAD_LENGTH + size);
    // CRC(4) | log_num(4) | seq(8) | type(1) | data_size(4) | data_payload

    // reserve crc, which is calculated in the end
    byte_order_utils::Writeu32_be(0, to_write_buffer);
    byte_order_utils::Writeu32_be(log_number_, to_write_buffer);
    ++log_number_;
    byte_order_utils::Writeu64_be(segment_id, to_write_buffer);
    to_write_buffer.push_back(type);
    byte_order_utils::Writeu32_be(size, to_write_buffer);
    if (size > 0) {
        to_write_buffer.append(entry, size);
    }

    uint32_t crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, (const Bytef*)(to_write_buffer.data() + 4), to_write_buffer.size() - 4);
    // write bigendian crc value to [0..4]
    for (auto i = 3; i >= 0; --i) {
        to_write_buffer[3 - i] = ((crc >> (8 * i)) & 0xff);
    }

    CETINA_COUNTER_INC(CONSENSUS_WAL_WRITE_COUNT);
    auto payload_size = to_write_buffer.size();
    CETINA_COUNTER_ADD(CONSENSUS_WAL_WRITE_BYTES, payload_size);
    segment_file_->write(to_write_buffer.data(), payload_size);
    if (segment_file_->fail()) {
        COBRE_ABORT();
    }
}

void NormalWalWriter::Sync() {
    segment_file_->flush();
    if (segment_file_->fail()) {
        COBRE_ABORT();
    }
}

void NormalWalWriter::Close() {
    if (segment_file_) {
        Sync();
        segment_file_->close();
        segment_file_.reset();
    }
}

}  // namespace wal
}  // namespace consensus_spec
