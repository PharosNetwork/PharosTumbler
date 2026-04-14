// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>
#include <experimental/filesystem>
#include <cstring>

#include "consensus/libraries/wal/wal.h"
#include "consensus/libraries/common/conversion.h"
#include "consensus/libraries/wal/byte_order_utils.h"

namespace consensus_spec {
namespace wal {

class WalTest : public testing::Test {
  public:
    static std::string Writeu64_beToString(uint64_t val) {
        std::string ret;
        byte_order_utils::Writeu64_be(val, ret);
        return ret;
    }

    static void ImportOldEntriesAndView(
        wal::WalLocalStorage& wal,
        const std::map<uint64_t, std::map<EntryType, std::vector<bytes>>>& old_entries,
        const std::pair<uint64_t, uint64_t>& seq_and_ts) {
        for (const auto& item : old_entries) {
            wal.Append(item.first, item.second);
        }
        wal.writer_->Sync();
        if (seq_and_ts.first > 0) {
            std::map<EntryType, std::vector<bytes>> seq_and_ts_entry;
            seq_and_ts_entry[EntryType::WAL_ENTRY_TYPE_TIMESTAMP].emplace_back(
                toBytes(Writeu64_beToString(seq_and_ts.second)));
            wal.Append(seq_and_ts.first, seq_and_ts_entry);
            wal.writer_->Sync();
        }

        wal.read_cache_.reset(new ReadCache());
        if (std::experimental::filesystem::exists(wal.old_file_name_)) {
            auto [_, highest] = wal.read_cache_->ReadLogFile(wal.old_file_name_, wal.option_.use_crc32_);
            wal.old_high_ = highest;
        }
        if (std::experimental::filesystem::exists(wal.current_file_name_)) {
            auto [_, highest] = wal.read_cache_->ReadLogFile(wal.current_file_name_, wal.option_.use_crc32_);
            wal.current_high_ = highest;
        }
        return;
    }
    void DoAppendTest() {
        // create
        auto db_path = "log/test_wal";
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);

        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        WAL_ERROR_CODE ret;
        {
            // first read seq and ts
            EXPECT_EQ(wal->GetLatestRawInfo().first, 0);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 0);
            // Append
            std::map<EntryType, std::vector<bytes>> to_append;
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message1"));
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message2"));
            to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result"));
            to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(123)));

            ret = wal->Append(1, to_append);
            EXPECT_EQ(ret, WAL_STATUS_OK);
            wal.reset();
        }

        {
            // reopen & GetEntries
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 1);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 123);

            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message1");
            EXPECT_EQ(entries[1], "message2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(entries.size(), 0);
            // wal.reset();
        }

        {
            // reopen & GetEntries & Append more
            // wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 1);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 123);

            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message1");
            EXPECT_EQ(entries[1], "message2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(entries.size(), 0);

            std::map<EntryType, std::vector<bytes>> to_append;
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message3"));
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message4"));
            to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result2"));
            to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(456)));

            ret = wal->Append(2, to_append);
            EXPECT_EQ(ret, WAL_STATUS_OK);
            wal.reset();
        }
        
        {
            // reopen & GetEntries
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 2);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 456);

            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message1");
            EXPECT_EQ(entries[1], "message2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message3");
            EXPECT_EQ(entries[1], "message4");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result2");

            wal.reset();
        }
        
        {
            // import old entries
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            std::map<uint64_t, std::map<EntryType, std::vector<bytes>>> to_import;
            to_import[3][WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message5"));
            to_import[4][WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result3"));
            std::string seq_and_ts;
            byte_order_utils::Writeu64_be(5, seq_and_ts);
            byte_order_utils::Writeu64_be(789, seq_and_ts);
            ImportOldEntriesAndView(*wal, to_import, {5, 789});

            EXPECT_EQ(wal->GetLatestRawInfo().first, 5);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 789);

            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message1");
            EXPECT_EQ(entries[1], "message2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message3");
            EXPECT_EQ(entries[1], "message4");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(3, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "message5");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(4, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result3");

            //roll
            EXPECT_FALSE(std::experimental::filesystem::exists(wal->old_file_name_));
            wal->Roll();
            EXPECT_TRUE(std::experimental::filesystem::exists(wal->old_file_name_));

            // append more
            std::map<EntryType, std::vector<bytes>> to_append;
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message6"));
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message7"));
            to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result4"));

            ret = wal->Append(5, to_append);

            // older (raw_seq, ts), should not be the latest
            to_append.clear();
            to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(101112)));
            ret = wal->Append(4, to_append);
            // wal.reset();
        }
        
        {
            // reopen after roll, roll again, append
            // wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 5);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 789);

            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message1");
            EXPECT_EQ(entries[1], "message2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message3");
            EXPECT_EQ(entries[1], "message4");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result2");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(3, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "message5");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(4, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result3");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(5, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message6");
            EXPECT_EQ(entries[1], "message7");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(5, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result4");

            // roll again
            wal->Roll();
            EXPECT_TRUE(std::experimental::filesystem::exists(wal->old_file_name_));
            EXPECT_TRUE(std::experimental::filesystem::exists(wal->current_file_name_));

            // append more
            std::map<EntryType, std::vector<bytes>> to_append;
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message8"));
            to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message9"));
            to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result5"));

            ret = wal->Append(6, to_append);
            wal.reset();
        }

        {
            // reopen after roll
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(3, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(4, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(entries.size(), 0);
            
            EXPECT_EQ(wal->GetEntries(5, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message6");
            EXPECT_EQ(entries[1], "message7");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(5, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result4");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(6, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 2);
            EXPECT_EQ(entries[0], "message8");
            EXPECT_EQ(entries[1], "message9");

            entries.clear();
            EXPECT_EQ(wal->GetEntries(5, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
            EXPECT_EQ(entries.size(), 1);
            EXPECT_EQ(entries[0], "consensus_result4");
        }

        // remove wal files
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);
    }

    void DoRollTest() {
        // create
        auto db_path = "log/test_wal_roll";
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);

        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        WAL_ERROR_CODE ret;
        {
            // Append 1-49
            for (uint64_t i = 1; i < 50; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(i+10000)));

                ret = wal->Append(i, to_append);
                EXPECT_EQ(ret, WAL_STATUS_OK);
            }
            EXPECT_FALSE(std::experimental::filesystem::exists(old_file_name));
            wal.reset();
        }
        {
            // read
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 49);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 49 + 10000);

            for (uint64_t i = 1; i < 50; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "message" + std::to_string(i));

                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
            }
            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(50, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            wal.reset();
        }
        {
            // read again, append 50 - 99
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 49);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 49 + 10000);

            for (uint64_t i = 1; i <= 49; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "message" + std::to_string(i));

                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
            }
            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(50, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);

            // append
            for (uint64_t i = 50; i <= 99; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(i + 10000)));

                ret = wal->Append(i, to_append);
                EXPECT_EQ(ret, WAL_STATUS_OK);
            }
            EXPECT_TRUE(std::experimental::filesystem::exists(old_file_name));
            wal.reset();
        }
        {
            // read 1-99
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 99);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 99 + 10000);

            for (uint64_t i = 1; i <= 99; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "message" + std::to_string(i));

                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
            }
            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(100, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            wal.reset();
        }
        {
            // Append 100-149, will roll
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            for (uint64_t i = 100; i <= 149; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(i+10000)));

                ret = wal->Append(i, to_append);
                EXPECT_EQ(ret, WAL_STATUS_OK);
            }
            EXPECT_TRUE(std::experimental::filesystem::exists(old_file_name));
            // wal.reset();
        }
        {
            // read, can only get 50 - 149
            // wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 149);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 149 + 10000);
            for (uint64_t i = 1; i <= 49; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_NO_DATA);
            }
            for (uint64_t i = 50; i <= 149; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "message" + std::to_string(i));

                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
            }
            wal.reset();
        }
        {
            // Append 150-199, will roll
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            for (uint64_t i = 150; i <= 199; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(i+10000)));

                ret = wal->Append(i, to_append);
                EXPECT_EQ(ret, WAL_STATUS_OK);
            }
            EXPECT_TRUE(std::experimental::filesystem::exists(old_file_name));
            wal.reset();
        }
        {
            // read, can only get 100 - 199
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            EXPECT_EQ(wal->GetLatestRawInfo().first, 199);
            EXPECT_EQ(byte_order_utils::Readu64_be(wal->GetLatestRawInfo().second), 199 + 10000);
            for (uint64_t i = 1; i <= 99; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_NO_DATA);
            }
            for (uint64_t i = 100; i <= 199; ++i) {
                std::vector<std::string> entries;
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "message" + std::to_string(i));

                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
            }
            wal.reset();
        }

        // remove wal files
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);
    }

    void ImportOldEntriesAndViewTest1() {
        // nothing at hand, import old
        // create
        auto db_path = "log/test_wal_import1";
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);

        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        {
            // Attention: entry %50 will cause roll
            std::map<uint64_t, std::map<EntryType, std::vector<bytes>>> old_entries;
            for (uint64_t i = 98; i <= 102; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                old_entries[i] = to_append;
            }
            std::pair<uint64_t, uint64_t> seq_and_ts = {102, 12000};
            ImportOldEntriesAndView(*wal, old_entries, seq_and_ts);
            
            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(97, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(103, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            for (uint64_t i = 98; i <= 102; ++i) {
                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "message" + std::to_string(i));

                entries.clear();
                EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                EXPECT_EQ(entries.size(), 1);
                EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
            }
            auto num_ts = wal->GetLatestRawInfo();
            EXPECT_EQ(num_ts.first, 102);
            EXPECT_EQ(byte_order_utils::Readu64_be(num_ts.second), 12000);
        }
    }

    void ImportOldEntriesAndViewTest2() {
        // nothing at hand, import nothing but seq
        // create
        auto db_path = "log/test_wal_import2";
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);

        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        {
            // Attention: entry 50 will cause roll
            std::map<uint64_t, std::map<EntryType, std::vector<bytes>>> old_entries;
            std::pair<uint64_t, uint64_t> seq_and_ts = {100, 12000};
            EXPECT_FALSE(std::experimental::filesystem::exists(wal->old_file_name_));
            ImportOldEntriesAndView(*wal, old_entries, seq_and_ts);
            EXPECT_TRUE(std::experimental::filesystem::exists(wal->old_file_name_));
            
            std::vector<std::string> entries;
            EXPECT_EQ(wal->GetEntries(100, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
            EXPECT_EQ(wal->GetEntries(100, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_NO_DATA);
            auto num_ts = wal->GetLatestRawInfo();
            EXPECT_EQ(num_ts.first, 100);
            EXPECT_EQ(byte_order_utils::Readu64_be(num_ts.second), 12000);
        }
    }

    void ImportOldEntriesAndViewTest3() {
        // have entries, import something
        // create
        auto db_path = "log/test_wal_import3";
        consensus_spec::wal::Option option;
        option.path_ = db_path;
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::string current_file_name = wal->current_file_name_;
        std::string old_file_name = wal->old_file_name_;
        wal.reset();
        std::experimental::filesystem::remove(current_file_name);
        std::experimental::filesystem::remove(old_file_name);

        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        WAL_ERROR_CODE ret;
        {
            // Append 100-150, will roll
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            for (uint64_t i = 100; i <= 150; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_TIMESTAMP].push_back(toBytes(Writeu64_beToString(i+10000)));

                ret = wal->Append(i, to_append);
                EXPECT_EQ(ret, WAL_STATUS_OK);
            }
            EXPECT_TRUE(std::experimental::filesystem::exists(old_file_name));
            wal.reset();
        }
        {
            wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
            // Attention: entry %50 will cause roll
            std::map<uint64_t, std::map<EntryType, std::vector<bytes>>> old_entries;
            for (uint64_t i = 98; i <= 102; ++i) {
                std::map<EntryType, std::vector<bytes>> to_append;
                to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("message" + std::to_string(i)));
                to_append[WAL_ENTRY_TYPE_CONSENSUS_RESULT].push_back(toBytes("consensus_result" + std::to_string(i)));
                old_entries[i] = to_append;
            }
            std::pair<uint64_t, uint64_t> seq_and_ts = {102, 12000};
            ImportOldEntriesAndView(*wal, old_entries, seq_and_ts);  // will be ignored
            
            auto num_ts = wal->GetLatestRawInfo();
            EXPECT_EQ(num_ts.first, 150);
            EXPECT_EQ(byte_order_utils::Readu64_be(num_ts.second), 10150);

            for (uint64_t i = 100; i <= 150; ++i) {
                std::vector<std::string> entries;
                if (i < 98 || i > 102) {
                    EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                    EXPECT_EQ(entries.size(), 1);
                    EXPECT_EQ(entries[0], "message" + std::to_string(i));

                    entries.clear();
                    EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                    EXPECT_EQ(entries.size(), 1);
                    EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
                } else {
                    EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
                    EXPECT_EQ(entries.size(), 2);
                    EXPECT_EQ(entries[0], "message" + std::to_string(i));

                    entries.clear();
                    EXPECT_EQ(wal->GetEntries(i, WAL_ENTRY_TYPE_CONSENSUS_RESULT, entries), WAL_STATUS_OK);
                    EXPECT_EQ(entries.size(), 2);
                    EXPECT_EQ(entries[0], "consensus_result" + std::to_string(i));
                }
            }
        }
    }
};

TEST_F(WalTest, AppendTest) {
    DoAppendTest();
}

TEST_F(WalTest, RollTest) {
    DoRollTest();
}

TEST_F(WalTest, ImportOldEntriesAndViewTest) {
    ImportOldEntriesAndViewTest1();
    ImportOldEntriesAndViewTest2();
    ImportOldEntriesAndViewTest3();
}

// Test byte order conversion functions
TEST_F(WalTest, ByteOrderConversionTest) {
    // Test byte_order_utils's Writeu32_be and Writeu64_be
    {
        std::string buffer;
        
        // Test Writeu32_be
        buffer.clear();
        byte_order_utils::Writeu32_be(0, buffer);
        EXPECT_EQ(buffer.size(), 4);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x00);
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x00);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x00);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x00);
        
        buffer.clear();
        byte_order_utils::Writeu32_be(0xFFFFFFFF, buffer);
        EXPECT_EQ(buffer.size(), 4);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0xFF);
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0xFF);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0xFF);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0xFF);
        
        buffer.clear();
        byte_order_utils::Writeu32_be(0x12345678, buffer);
        EXPECT_EQ(buffer.size(), 4);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x12);
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x34);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x56);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x78);
        
        buffer.clear();
        byte_order_utils::Writeu32_be(0x01020304, buffer);
        EXPECT_EQ(buffer.size(), 4);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x01);
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x02);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x03);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x04);
    }
    
    // Test byte_order_utils's Writeu64_be
    {
        std::string buffer;
        
        buffer.clear();
        byte_order_utils::Writeu64_be(0, buffer);
        EXPECT_EQ(buffer.size(), 8);
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(static_cast<uint8_t>(buffer[i]), 0x00);
        }
        
        buffer.clear();
        byte_order_utils::Writeu64_be(0xFFFFFFFFFFFFFFFF, buffer);
        EXPECT_EQ(buffer.size(), 8);
        for (int i = 0; i < 8; ++i) {
            EXPECT_EQ(static_cast<uint8_t>(buffer[i]), 0xFF);
        }
        
        buffer.clear();
        byte_order_utils::Writeu64_be(0x123456789ABCDEF0, buffer);
        EXPECT_EQ(buffer.size(), 8);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x12);
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x34);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x56);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x78);
        EXPECT_EQ(static_cast<uint8_t>(buffer[4]), 0x9A);
        EXPECT_EQ(static_cast<uint8_t>(buffer[5]), 0xBC);
        EXPECT_EQ(static_cast<uint8_t>(buffer[6]), 0xDE);
        EXPECT_EQ(static_cast<uint8_t>(buffer[7]), 0xF0);
        
        buffer.clear();
        byte_order_utils::Writeu64_be(0x0102030405060708, buffer);
        EXPECT_EQ(buffer.size(), 8);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x01);
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x02);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x03);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x04);
        EXPECT_EQ(static_cast<uint8_t>(buffer[4]), 0x05);
        EXPECT_EQ(static_cast<uint8_t>(buffer[5]), 0x06);
        EXPECT_EQ(static_cast<uint8_t>(buffer[6]), 0x07);
        EXPECT_EQ(static_cast<uint8_t>(buffer[7]), 0x08);
    }
    
    // Test byte_order_utils's Readu32_be and Readu64_be
    {
        // Test Readu32_be
        std::string data = "\x00\x00\x00\x00";
        uint32_t value = byte_order_utils::Readu32_be(std::string_view(data));
        EXPECT_EQ(value, 0);
        
        data = "\xFF\xFF\xFF\xFF";
        value = byte_order_utils::Readu32_be(std::string_view(data));
        EXPECT_EQ(value, 0xFFFFFFFF);
        
        data = "\x12\x34\x56\x78";
        value = byte_order_utils::Readu32_be(std::string_view(data));
        EXPECT_EQ(value, 0x12345678);
        
        data = "\x01\x02\x03\x04";
        value = byte_order_utils::Readu32_be(std::string_view(data));
        EXPECT_EQ(value, 0x01020304);
    }
    
    // Test byte_order_utils's Readu64_be
    {
        std::string data = "\x00\x00\x00\x00\x00\x00\x00\x00";
        uint64_t value = byte_order_utils::Readu64_be(std::string_view(data));
        EXPECT_EQ(value, 0);
        
        data = "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
        value = byte_order_utils::Readu64_be(std::string_view(data));
        EXPECT_EQ(value, 0xFFFFFFFFFFFFFFFF);
        
        data = "\x12\x34\x56\x78\x9A\xBC\xDE\xF0";
        value = byte_order_utils::Readu64_be(std::string_view(data));
        EXPECT_EQ(value, 0x123456789ABCDEF0);
        
        data = "\x01\x02\x03\x04\x05\x06\x07\x08";
        value = byte_order_utils::Readu64_be(std::string_view(data));
        EXPECT_EQ(value, 0x0102030405060708);
    }
    
    // Test round-trip consistency: write then read
    {
        std::string buffer;
        uint32_t original_values[] = {
            0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF,
            0x00000001, 0x000000FF, 0x0000FFFF, 0x00FFFFFF,
            0x12345678, 0x87654321, 0xDEADBEEF, 0xCAFEBABE,
            0x11111111, 0x22222222, 0x33333333, 0x44444444,
            0x55555555, 0x66666666, 0x77777777, 0x88888888,
            0x99999999, 0xAAAAAAAA, 0xBBBBBBBB, 0xCCCCCCCC,
            0xDDDDDDDD, 0xEEEEEEEE, 0x01020304, 0x04030201
        };
        
        for (uint32_t original : original_values) {
            buffer.clear();
            byte_order_utils::Writeu32_be(original, buffer);
            EXPECT_EQ(buffer.size(), 4);
            
            uint32_t read_back = byte_order_utils::Readu32_be(std::string_view(buffer));
            EXPECT_EQ(read_back, original);
        }
        
        uint64_t original_values64[] = {
            0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF,
            0x0000000000000001, 0x00000000000000FF, 0x000000000000FFFF, 0x00000000FFFFFFFF,
            0x00000001FFFFFFFF, 0x0000FFFFFFFFFFFF, 0x00FFFFFFFFFFFFFF,
            0x123456789ABCDEF0, 0xFEDCBA0987654321, 0xDEADBEEFCAFEBABE, 0xCAFEBABEDEADBEEF,
            0x1111111111111111, 0x2222222222222222, 0x3333333333333333, 0x4444444444444444,
            0x5555555555555555, 0x6666666666666666, 0x7777777777777777, 0x8888888888888888,
            0x9999999999999999, 0xAAAAAAAAAAAAAAAA, 0xBBBBBBBBBBBBBBBB, 0xCCCCCCCCCCCCCCCC,
            0xDDDDDDDDDDDDDDDD, 0xEEEEEEEEEEEEEEEE, 0x0102030405060708, 0x0807060504030201,
            0x1234567890ABCDEF, 0xFEDCBA0987654321, 0x0000000000001234, 0x00000000ABCDEF00,
            0x1234000000000000, 0x000000000000000A, 0x00000000000000FF, 0x000000000000FFFF,
            0x0000000000FFFFFF, 0x00000000FFFFFFFF, 0x000000FFFFFFFFFF, 0x0000FFFFFFFFFFFF,
            0x00FFFFFFFFFFFFFF, 0xAAAAAAAA55555555, 0x55555555AAAAAAAA, 0x1234567887654321
        };
        
        for (uint64_t original : original_values64) {
            buffer.clear();
            byte_order_utils::Writeu64_be(original, buffer);
            EXPECT_EQ(buffer.size(), 8);
            
            uint64_t read_back = byte_order_utils::Readu64_be(std::string_view(buffer));
            EXPECT_EQ(read_back, original);
        }
    }
    
    // Test boundary values
    {
        std::string buffer;
        
        // Test minimum 32-bit value
        buffer.clear();
        byte_order_utils::Writeu32_be(0, buffer);
        uint32_t min_read = byte_order_utils::Readu32_be(std::string_view(buffer));
        EXPECT_EQ(min_read, 0);
        
        // Test maximum 32-bit value
        buffer.clear();
        byte_order_utils::Writeu32_be(0xFFFFFFFF, buffer);
        uint32_t max_read = byte_order_utils::Readu32_be(std::string_view(buffer));
        EXPECT_EQ(max_read, 0xFFFFFFFF);
        
        // Test middle range values
        buffer.clear();
        byte_order_utils::Writeu32_be(0x80000000, buffer);
        uint32_t mid_read = byte_order_utils::Readu32_be(std::string_view(buffer));
        EXPECT_EQ(mid_read, 0x80000000);
        
        // Test minimum 64-bit value
        buffer.clear();
        byte_order_utils::Writeu64_be(0, buffer);
        uint64_t min_read64 = byte_order_utils::Readu64_be(std::string_view(buffer));
        EXPECT_EQ(min_read64, 0);
        
        // Test maximum 64-bit value
        buffer.clear();
        byte_order_utils::Writeu64_be(0xFFFFFFFFFFFFFFFF, buffer);
        uint64_t max_read64 = byte_order_utils::Readu64_be(std::string_view(buffer));
        EXPECT_EQ(max_read64, 0xFFFFFFFFFFFFFFFF);
        
        // Test middle range values
        buffer.clear();
        byte_order_utils::Writeu64_be(0x8000000000000000, buffer);
        uint64_t mid_read64 = byte_order_utils::Readu64_be(std::string_view(buffer));
        EXPECT_EQ(mid_read64, 0x8000000000000000);
    }
    
    // Test byte order correctness with specific byte patterns
    {
        std::string buffer;
        
        // Test 32-bit byte order
        buffer.clear();
        byte_order_utils::Writeu32_be(0x01020304, buffer);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x01);  // MSB
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x02);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x03);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x04);  // LSB
        
        uint32_t value = byte_order_utils::Readu32_be(std::string_view(buffer));
        EXPECT_EQ(value, 0x01020304);
        
        // Test 64-bit byte order
        buffer.clear();
        byte_order_utils::Writeu64_be(0x0102030405060708, buffer);
        EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0x01);  // MSB
        EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0x02);
        EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x03);
        EXPECT_EQ(static_cast<uint8_t>(buffer[3]), 0x04);
        EXPECT_EQ(static_cast<uint8_t>(buffer[4]), 0x05);
        EXPECT_EQ(static_cast<uint8_t>(buffer[5]), 0x06);
        EXPECT_EQ(static_cast<uint8_t>(buffer[6]), 0x07);
        EXPECT_EQ(static_cast<uint8_t>(buffer[7]), 0x08);  // LSB
        
        uint64_t value64 = byte_order_utils::Readu64_be(std::string_view(buffer));
        EXPECT_EQ(value64, 0x0102030405060708);
    }
    
    // Test continuous write and read of multiple u32 and u64 values
    {
        std::string buffer;
        
        // Test data from round-trip consistency test cases
        uint32_t u32_values[] = {
            0, 1, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF,
            0x00000001, 0x000000FF, 0x0000FFFF, 0x00FFFFFF,
            0x12345678, 0x87654321, 0xDEADBEEF, 0xCAFEBABE,
            0x11111111, 0x22222222, 0x33333333, 0x44444444,
            0x55555555, 0x66666666, 0x77777777, 0x88888888,
            0x99999999, 0xAAAAAAAA, 0xBBBBBBBB, 0xCCCCCCCC,
            0xDDDDDDDD, 0xEEEEEEEE, 0x01020304, 0x04030201
        };
        
        uint64_t u64_values[] = {
            0, 1, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, 0xFFFFFFFFFFFFFFFF,
            0x0000000000000001, 0x00000000000000FF, 0x000000000000FFFF, 0x00000000FFFFFFFF,
            0x00000001FFFFFFFF, 0x0000FFFFFFFFFFFF, 0x00FFFFFFFFFFFFFF,
            0x123456789ABCDEF0, 0xFEDCBA0987654321, 0xDEADBEEFCAFEBABE, 0xCAFEBABEDEADBEEF,
            0x1111111111111111, 0x2222222222222222, 0x3333333333333333, 0x4444444444444444,
            0x5555555555555555, 0x6666666666666666, 0x7777777777777777, 0x8888888888888888,
            0x9999999999999999, 0xAAAAAAAAAAAAAAAA, 0xBBBBBBBBBBBBBBBB, 0xCCCCCCCCCCCCCCCC,
            0xDDDDDDDDDDDDDDDD, 0xEEEEEEEEEEEEEEEE, 0x0102030405060708, 0x0807060504030201,
            0x1234567890ABCDEF, 0xFEDCBA0987654321, 0x0000000000001234, 0x00000000ABCDEF00,
            0x1234000000000000, 0x000000000000000A, 0x00000000000000FF, 0x000000000000FFFF,
            0x0000000000FFFFFF, 0x00000000FFFFFFFF, 0x000000FFFFFFFFFF, 0x0000FFFFFFFFFFFF,
            0x00FFFFFFFFFFFFFF, 0xAAAAAAAA55555555, 0x55555555AAAAAAAA, 0x1234567887654321
        };
        
        // Continuous write: u32 values followed by u64 values
        buffer.clear();
        for (uint32_t value : u32_values) {
            byte_order_utils::Writeu32_be(value, buffer);
        }
        for (uint64_t value : u64_values) {
            byte_order_utils::Writeu64_be(value, buffer);
        }
        
        // Verify buffer size
        size_t expected_size = sizeof(u32_values) + sizeof(u64_values);
        EXPECT_EQ(buffer.size(), expected_size);
        
        // Continuous read: read back all u32 values first, then u64 values
        std::string_view buffer_view(buffer);
        size_t offset = 0;
        
        // Read back u32 values
        for (size_t i = 0; i < sizeof(u32_values) / sizeof(u32_values[0]); ++i) {
            EXPECT_LT(offset + 4, buffer.size() + 1);
            std::string_view value_view(buffer_view.substr(offset, 4));
            uint32_t read_value = byte_order_utils::Readu32_be(value_view);
            EXPECT_EQ(read_value, u32_values[i]);
            offset += 4;
        }
        
        // Read back u64 values
        for (size_t i = 0; i < sizeof(u64_values) / sizeof(u64_values[0]); ++i) {
            EXPECT_LT(offset + 8, buffer.size() + 1);
            std::string_view value_view(buffer_view.substr(offset, 8));
            uint64_t read_value = byte_order_utils::Readu64_be(value_view);
            EXPECT_EQ(read_value, u64_values[i]);
            offset += 8;
        }
        
        EXPECT_EQ(offset, buffer.size());
        
        // Test mixed pattern: interleave u32 and u64 values from the full arrays
        buffer.clear();
        size_t u32_count = sizeof(u32_values) / sizeof(u32_values[0]);
        size_t u64_count = sizeof(u64_values) / sizeof(u64_values[0]);
        size_t min_count = std::min(u32_count, u64_count);
        
        // Interleave u32 and u64 values
        for (size_t i = 0; i < min_count; ++i) {
            byte_order_utils::Writeu32_be(u32_values[i], buffer);
            byte_order_utils::Writeu64_be(u64_values[i], buffer);
        }
        
        // Write remaining values if arrays have different lengths
        for (size_t i = min_count; i < u32_count; ++i) {
            byte_order_utils::Writeu32_be(u32_values[i], buffer);
        }
        for (size_t i = min_count; i < u64_count; ++i) {
            byte_order_utils::Writeu64_be(u64_values[i], buffer);
        }
        
        // Read back interleaved pattern
        buffer_view = std::string_view(buffer);
        offset = 0;
        
        // Read back interleaved u32 and u64 values
        for (size_t i = 0; i < min_count; ++i) {
            std::string_view u32_view(buffer_view.substr(offset, 4));
            uint32_t u32_read = byte_order_utils::Readu32_be(u32_view);
            EXPECT_EQ(u32_read, u32_values[i]);
            offset += 4;
            
            std::string_view u64_view(buffer_view.substr(offset, 8));
            uint64_t u64_read = byte_order_utils::Readu64_be(u64_view);
            EXPECT_EQ(u64_read, u64_values[i]);
            offset += 8;
        }
        
        // Read remaining u32 values
        for (size_t i = min_count; i < u32_count; ++i) {
            std::string_view u32_view(buffer_view.substr(offset, 4));
            uint32_t u32_read = byte_order_utils::Readu32_be(u32_view);
            EXPECT_EQ(u32_read, u32_values[i]);
            offset += 4;
        }
        
        // Read remaining u64 values
        for (size_t i = min_count; i < u64_count; ++i) {
            std::string_view u64_view(buffer_view.substr(offset, 8));
            uint64_t u64_read = byte_order_utils::Readu64_be(u64_view);
            EXPECT_EQ(u64_read, u64_values[i]);
            offset += 8;
        }
        
        EXPECT_EQ(offset, buffer.size());
    }
}

// Test read_cache error handling for corrupted WAL files
TEST_F(WalTest, ReadCacheCorruptedFileTest) {
    std::string db_path = "log/test_wal_read_cache_corrupted";
    consensus_spec::wal::Option option;
    option.path_ = db_path;
    option.use_crc32_ = true;
    option.sync_ = true;
    
    std::string current_file_name = db_path + "/CURRENT";
    std::string old_file_name = db_path + "/OLD";
    
    std::experimental::filesystem::create_directories(db_path);
    std::experimental::filesystem::remove(current_file_name);
    std::experimental::filesystem::remove(old_file_name);

    // Test case 1: Incomplete entry head (line 31 in read_cache.cpp)
    {
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        
        // Write a valid entry first
        std::map<EntryType, std::vector<bytes>> to_append;
        to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("valid_message"));
        EXPECT_EQ(wal->Append(1, to_append), WAL_STATUS_OK);
        wal.reset();
        
        // Ensure file exists before getting size
        EXPECT_TRUE(std::experimental::filesystem::exists(current_file_name));
        auto file_size = std::experimental::filesystem::file_size(current_file_name);
        
        // Append incomplete head data (only 10 bytes instead of 21)
        std::ofstream out(current_file_name, std::ios::app | std::ios::binary);
        char incomplete_head[10] = {0};
        out.write(incomplete_head, 10);
        out.close();
        
        // Try to read, should handle incomplete head gracefully
        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::vector<std::string> entries;
        EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
        EXPECT_EQ(entries.size(), 1);
        EXPECT_EQ(entries[0], "valid_message");
        
        // File should be truncated to remove incomplete data
        auto new_size = std::experimental::filesystem::file_size(current_file_name);
        EXPECT_EQ(new_size, file_size);
        
        wal.reset();
    }
    
    std::experimental::filesystem::remove(current_file_name);
    
    // Test case 2: Incomplete entry payload (line 51 in read_cache.cpp)
    {
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        
        // Write a valid entry
        std::map<EntryType, std::vector<bytes>> to_append;
        to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("valid_message2"));
        EXPECT_EQ(wal->Append(1, to_append), WAL_STATUS_OK);
        wal.reset();
        
        // Ensure file exists before getting size
        EXPECT_TRUE(std::experimental::filesystem::exists(current_file_name));
        auto file_size = std::experimental::filesystem::file_size(current_file_name);
        
        // Manually create an entry with incomplete payload
        std::ofstream out(current_file_name, std::ios::app | std::ios::binary);
        
        // Create entry head with large payload size but don't write full payload
        char head[ENTRY_HEAD_LENGTH];
        memset(head, 0, ENTRY_HEAD_LENGTH);
        
        // CRC placeholder (4 bytes)
        uint32_t dummy_crc = 0;
        memcpy(head, &dummy_crc, 4);
        
        // log_num = 1 (4 bytes)
        uint32_t log_num = 1;
        std::string log_num_str;
        byte_order_utils::Writeu32_be(log_num, log_num_str);
        memcpy(&head[4], log_num_str.data(), 4);
        
        // segment_id = 2 (8 bytes)
        uint64_t segment_id = 2;
        std::string segment_str;
        byte_order_utils::Writeu64_be(segment_id, segment_str);
        memcpy(&head[8], segment_str.data(), 8);
        
        // type = MESSAGE (1 byte)
        head[16] = static_cast<char>(WAL_ENTRY_TYPE_MESSAGE);
        
        // size = 1000 (but we won't write 1000 bytes) (4 bytes)
        uint32_t payload_size = 1000;
        std::string size_str;
        byte_order_utils::Writeu32_be(payload_size, size_str);
        memcpy(&head[17], size_str.data(), 4);
        
        out.write(head, ENTRY_HEAD_LENGTH);
        // Only write 50 bytes instead of 1000
        char partial_data[50] = {0};
        out.write(partial_data, 50);
        out.close();
        
        // Try to read, should handle incomplete payload gracefully
        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::vector<std::string> entries;
        EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
        EXPECT_EQ(entries.size(), 1);
        EXPECT_EQ(entries[0], "valid_message2");
        
        // Should not have entry for segment 2 due to corruption
        entries.clear();
        EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
        
        // File should be truncated
        auto new_size = std::experimental::filesystem::file_size(current_file_name);
        EXPECT_EQ(new_size, file_size);
        
        wal.reset();
    }
    
    std::experimental::filesystem::remove(current_file_name);
    
    // Test case 3: CRC mismatch (line 66 in read_cache.cpp)
    {
        auto wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        
        // Write a valid entry
        std::map<EntryType, std::vector<bytes>> to_append;
        to_append[WAL_ENTRY_TYPE_MESSAGE].push_back(toBytes("valid_message3"));
        EXPECT_EQ(wal->Append(1, to_append), WAL_STATUS_OK);
        wal.reset();
        
        // Ensure file exists before getting size
        EXPECT_TRUE(std::experimental::filesystem::exists(current_file_name));
        auto file_size = std::experimental::filesystem::file_size(current_file_name);
        
        // Manually create an entry with wrong CRC
        std::ofstream out(current_file_name, std::ios::app | std::ios::binary);
        
        std::string data = "corrupted_message";
        char head[ENTRY_HEAD_LENGTH];
        memset(head, 0, ENTRY_HEAD_LENGTH);
        
        // Write WRONG CRC (just use 0xFFFFFFFF)
        uint32_t wrong_crc = 0xFFFFFFFF;
        std::string crc_str;
        byte_order_utils::Writeu32_be(wrong_crc, crc_str);
        memcpy(head, crc_str.data(), 4);
        
        // log_num = 1
        uint32_t log_num = 1;
        std::string log_num_str;
        byte_order_utils::Writeu32_be(log_num, log_num_str);
        memcpy(&head[4], log_num_str.data(), 4);
        
        // segment_id = 2
        uint64_t segment_id = 2;
        std::string segment_str;
        byte_order_utils::Writeu64_be(segment_id, segment_str);
        memcpy(&head[8], segment_str.data(), 8);
        
        // type = MESSAGE
        head[16] = static_cast<char>(WAL_ENTRY_TYPE_MESSAGE);
        
        // size
        uint32_t payload_size = data.size();
        std::string size_str;
        byte_order_utils::Writeu32_be(payload_size, size_str);
        memcpy(&head[17], size_str.data(), 4);
        
        out.write(head, ENTRY_HEAD_LENGTH);
        out.write(data.data(), data.size());
        out.close();
        
        // Try to read with CRC check enabled, should detect corruption
        wal = std::unique_ptr<wal::WalLocalStorage>(new wal::WalLocalStorage(option));
        std::vector<std::string> entries;
        EXPECT_EQ(wal->GetEntries(1, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_OK);
        EXPECT_EQ(entries.size(), 1);
        EXPECT_EQ(entries[0], "valid_message3");
        
        // Should not have entry for segment 2 due to CRC error
        entries.clear();
        EXPECT_EQ(wal->GetEntries(2, WAL_ENTRY_TYPE_MESSAGE, entries), WAL_STATUS_NO_DATA);
        
        // File should be truncated to remove corrupted entry
        auto new_size = std::experimental::filesystem::file_size(current_file_name);
        EXPECT_EQ(new_size, file_size);
        
        wal.reset();
    }
    
    std::experimental::filesystem::remove(current_file_name);
    std::experimental::filesystem::remove(old_file_name);
    std::experimental::filesystem::remove_all(db_path);
}

}  // namespace wal
}  // namespace consensus_spec
