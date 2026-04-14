// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "consensus/common/consensus_common.h"

namespace consensus_spec {

class ConsensusCommonTest : public testing::Test {};

TEST_F(ConsensusCommonTest, ProposalKeyTest) {
    ProposalKey k1(2, NodeId("0"));
    ProposalKey k2(0, NodeId("2"));
    ProposalKey k3(0, NodeId("2"));
    EXPECT_TRUE(!(k1 < k2));
    EXPECT_TRUE(k2 == k3);
}


// Test conversion.h inline functions for code coverage
class ConversionTest : public testing::Test {};

TEST_F(ConversionTest, ToHexFromUint8Array) {
    // Test toHex(const uint8_t* data, int length)
    uint8_t data[] = {0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
    std::string hex = toHex(data, 8);
    EXPECT_EQ(hex, "0123456789abcdef");
    
    // Test empty data
    std::string empty_hex = toHex(data, 0);
    EXPECT_EQ(empty_hex, "");
    
    // Test single byte
    uint8_t single[] = {0xff};
    std::string single_hex = toHex(single, 1);
    EXPECT_EQ(single_hex, "ff");
}

TEST_F(ConversionTest, ToHexFromCharArray) {
    // Test toHex(const char* data, int length)
    const char* data = "hello";
    std::string hex = toHex(data, 5);
    EXPECT_EQ(hex, "68656c6c6f");
    
    // Test empty data
    std::string empty_hex = toHex(data, 0);
    EXPECT_EQ(empty_hex, "");
}

TEST_F(ConversionTest, ToHexFromString) {
    // Test toHex(const std::string& data)
    std::string data = "test";
    std::string hex = toHex(data);
    EXPECT_EQ(hex, "74657374");
    
    // Test empty string
    std::string empty_data = "";
    std::string empty_hex = toHex(empty_data);
    EXPECT_EQ(empty_hex, "");
    
    // Test string with special characters (use constructor to avoid null termination)
    std::string special(4, '\0');
    special[0] = 0x00;
    special[1] = 0x01;
    special[2] = 0x02;
    special[3] = 0x03;
    std::string special_hex = toHex(special);
    EXPECT_EQ(special_hex, "00010203");
}

TEST_F(ConversionTest, ToHexFromStringView) {
    // Test toHex(const std::string_view& data)
    std::string_view data = "world";
    std::string hex = toHex(data);
    EXPECT_EQ(hex, "776f726c64");
    
    // Test empty string_view
    std::string_view empty_data = "";
    std::string empty_hex = toHex(empty_data);
    EXPECT_EQ(empty_hex, "");
}

TEST_F(ConversionTest, ToHexFromBytesVector) {
    // Test toHex(const std::vector<uint8_t>& data)
    std::vector<uint8_t> data = {0xde, 0xad, 0xbe, 0xef};
    std::string hex = toHex(data);
    EXPECT_EQ(hex, "deadbeef");
    
    // Test empty vector
    std::vector<uint8_t> empty_data;
    std::string empty_hex = toHex(empty_data);
    EXPECT_EQ(empty_hex, "");
    
    // Test vector with all possible byte values (sample)
    std::vector<uint8_t> all_bytes = {0x00, 0x0f, 0xf0, 0xff};
    std::string all_hex = toHex(all_bytes);
    EXPECT_EQ(all_hex, "000ff0ff");
}

TEST_F(ConversionTest, ToBytesFromString) {
    // Test toBytes(const std::string& data)
    std::string data = "hello";
    bytes result = toBytes(data);
    
    EXPECT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], 'h');
    EXPECT_EQ(result[1], 'e');
    EXPECT_EQ(result[2], 'l');
    EXPECT_EQ(result[3], 'l');
    EXPECT_EQ(result[4], 'o');
    
    // Test empty string
    std::string empty_data = "";
    bytes empty_result = toBytes(empty_data);
    EXPECT_EQ(empty_result.size(), 0);
    
    // Test string with binary data (use constructor to avoid null termination)
    std::string binary_data(5, '\0');
    binary_data[0] = 0x00;
    binary_data[1] = 0x01;
    binary_data[2] = 0x02;
    binary_data[3] = 0x03;
    binary_data[4] = 0x04;
    bytes binary_result = toBytes(binary_data);
    EXPECT_EQ(binary_result.size(), 5);
    EXPECT_EQ(binary_result[0], 0x00);
    EXPECT_EQ(binary_result[1], 0x01);
    EXPECT_EQ(binary_result[2], 0x02);
    EXPECT_EQ(binary_result[3], 0x03);
    EXPECT_EQ(binary_result[4], 0x04);
}

TEST_F(ConversionTest, ToHexAndToBytesRoundTrip) {
    // Test round-trip conversion: string -> bytes -> hex -> bytes
    std::string original = "test data";
    bytes bytes_data = toBytes(original);
    std::string hex = toHex(bytes_data);
    
    // Verify hex is correct (9 characters = 18 hex digits)
    // "test data" = 74 65 73 74 20 64 61 74 61
    EXPECT_EQ(hex, "746573742064617461");
    
    // Verify bytes match original
    EXPECT_EQ(bytes_data.size(), original.size());
    for (size_t i = 0; i < original.size(); ++i) {
        EXPECT_EQ(bytes_data[i], static_cast<uint8_t>(original[i]));
    }
}

}  // namespace consensus_spec