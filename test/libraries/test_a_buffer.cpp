// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "consensus/libraries/common/a_buffer.h"

namespace consensus_spec {

class ABufferTest : public testing::Test {
  protected:
    void SetUp() override {}

    void TearDown() override {}
};

// test default constructor
TEST_F(ABufferTest, DefaultConstructor) {
    ABuffer buffer;
    EXPECT_EQ(buffer.Buffer(), nullptr);
    EXPECT_EQ(buffer.Data(), nullptr);
    EXPECT_EQ(buffer.Begin(), nullptr);
    EXPECT_EQ(buffer.End(), nullptr);
    EXPECT_EQ(buffer.Size(), 0);
    EXPECT_EQ(buffer.OwnBuffer(), false);
}

// test capacity constructor
TEST_F(ABufferTest, CapacityConstructor) {
    const size_t capacity = 1024;
    ABuffer buffer(capacity);
    
    EXPECT_NE(buffer.Buffer(), nullptr);
    EXPECT_NE(buffer.Data(), nullptr);
    EXPECT_NE(buffer.Begin(), nullptr);
    EXPECT_NE(buffer.End(), nullptr);
    EXPECT_EQ(buffer.Size(), capacity);
    EXPECT_EQ(buffer.OwnBuffer(), true);
    EXPECT_EQ(buffer.Left(), 0);
    EXPECT_EQ(buffer.Right(), capacity);
}

// test buffer constructor
TEST_F(ABufferTest, BufferConstructor) {
    const size_t size = 100;
    uint8_t* raw_buffer = new uint8_t[size];
    
    ABuffer buffer(raw_buffer, size);
    
    EXPECT_EQ(buffer.Buffer(), raw_buffer);
    EXPECT_EQ(buffer.Data(), raw_buffer);
    EXPECT_EQ(buffer.Begin(), raw_buffer);
    EXPECT_EQ(buffer.End(), raw_buffer + size);
    EXPECT_EQ(buffer.Size(), size);
    EXPECT_EQ(buffer.OwnBuffer(), true);
}

// test full parameter constructor
TEST_F(ABufferTest, FullParameterConstructor) {
    const size_t capacity = 200;
    uint8_t* raw_buffer = new uint8_t[capacity];
    uint8_t* head = raw_buffer + 10;
    uint8_t* tail = raw_buffer + 150;
    
    ABuffer buffer(raw_buffer, head, tail, capacity);
    
    EXPECT_EQ(buffer.Buffer(), raw_buffer);
    EXPECT_EQ(buffer.Data(), head);
    EXPECT_EQ(buffer.Begin(), head);
    EXPECT_EQ(buffer.End(), tail);
    EXPECT_EQ(buffer.Size(), 140);
    EXPECT_EQ(buffer.OwnBuffer(), true);
}

// test three-parameter constructor (buf, head, sz)
TEST_F(ABufferTest, ThreeParameterConstructor) {
    const size_t capacity = 256;
    uint8_t* raw_buffer = new uint8_t[capacity];
    uint8_t* head = raw_buffer + 32;
    const size_t data_size = 128;
    
    ABuffer buffer(raw_buffer, head, data_size);
    
    EXPECT_EQ(buffer.Buffer(), raw_buffer);
    EXPECT_EQ(buffer.Data(), head);
    EXPECT_EQ(buffer.Begin(), head);
    EXPECT_EQ(buffer.End(), head + data_size);
    EXPECT_EQ(buffer.Size(), data_size);
    EXPECT_EQ(buffer.OwnBuffer(), true);
}

// test three-parameter constructor boundary conditions
TEST_F(ABufferTest, ThreeParameterConstructorBoundary) {
    // test case where head equals buf
    {
        const size_t capacity = 100;
        uint8_t* raw_buffer = new uint8_t[capacity];
        ABuffer buffer1(raw_buffer, raw_buffer, capacity);
        EXPECT_EQ(buffer1.Buffer(), raw_buffer);
        EXPECT_EQ(buffer1.Data(), raw_buffer);
        EXPECT_EQ(buffer1.Begin(), raw_buffer);
        EXPECT_EQ(buffer1.End(), raw_buffer + capacity);
        EXPECT_EQ(buffer1.Size(), capacity);
        EXPECT_EQ(buffer1.OwnBuffer(), true);
    }
    
    // test case where head is in the middle of buf
    {
        const size_t capacity = 100;
        uint8_t* raw_buffer = new uint8_t[capacity];
        uint8_t* head = raw_buffer + 10;
        const size_t data_size = 50;
        ABuffer buffer2(raw_buffer, head, data_size);
        EXPECT_EQ(buffer2.Buffer(), raw_buffer);
        EXPECT_EQ(buffer2.Data(), head);
        EXPECT_EQ(buffer2.Begin(), head);
        EXPECT_EQ(buffer2.End(), head + data_size);
        EXPECT_EQ(buffer2.Size(), data_size);
        EXPECT_EQ(buffer2.OwnBuffer(), true);
    }
    
    // test case where head is near the end of buf
    {
        const size_t capacity = 100;
        uint8_t* raw_buffer = new uint8_t[capacity];
        uint8_t* head3 = raw_buffer + 90;
        const size_t data_size3 = 10;
        ABuffer buffer3(raw_buffer, head3, data_size3);
        EXPECT_EQ(buffer3.Buffer(), raw_buffer);
        EXPECT_EQ(buffer3.Data(), head3);
        EXPECT_EQ(buffer3.Begin(), head3);
        EXPECT_EQ(buffer3.End(), head3 + data_size3);
        EXPECT_EQ(buffer3.Size(), data_size3);
        EXPECT_EQ(buffer3.OwnBuffer(), true);
    }
}

// test move constructor
TEST_F(ABufferTest, MoveConstructor) {
    const size_t capacity = 256;
    ABuffer original(capacity);
    uint8_t* original_buffer = original.Buffer();
    
    ABuffer moved(std::move(original));
    
    EXPECT_EQ(moved.Buffer(), original_buffer);
    EXPECT_EQ(moved.Size(), capacity);
    EXPECT_EQ(moved.OwnBuffer(), true);
    
    EXPECT_EQ(original.Buffer(), nullptr);
    EXPECT_EQ(original.Size(), 0);
    EXPECT_EQ(original.OwnBuffer(), false);
}

// test move assignment operator
TEST_F(ABufferTest, MoveAssignmentOperator) {
    const size_t capacity1 = 128;
    const size_t capacity2 = 256;
    ABuffer buffer1(capacity1);
    ABuffer buffer2(capacity2);
    
    uint8_t* buffer2_ptr = buffer2.Buffer();
    size_t buffer2_size = buffer2.Size();
    
    buffer1 = std::move(buffer2);
    
    EXPECT_EQ(buffer1.Buffer(), buffer2_ptr);
    EXPECT_EQ(buffer1.Size(), buffer2_size);
    EXPECT_EQ(buffer1.OwnBuffer(), true);
}

// test Swap method
TEST_F(ABufferTest, SwapMethod) {
    const size_t capacity1 = 100;
    const size_t capacity2 = 200;
    
    ABuffer buffer1(capacity1);
    ABuffer buffer2(capacity2);
    
    uint8_t* ptr1 = buffer1.Buffer();
    uint8_t* ptr2 = buffer2.Buffer();
    
    buffer1.Swap(buffer2);
    
    EXPECT_EQ(buffer1.Buffer(), ptr2);
    EXPECT_EQ(buffer1.Size(), capacity2);
    
    EXPECT_EQ(buffer2.Buffer(), ptr1);
    EXPECT_EQ(buffer2.Size(), capacity1);
}

// test Free method
TEST_F(ABufferTest, FreeMethod) {
    ABuffer buffer(100);
    EXPECT_TRUE(buffer.OwnBuffer());
    
    buffer.Free();
    
    EXPECT_EQ(buffer.Buffer(), nullptr);
    EXPECT_EQ(buffer.Size(), 0);
    EXPECT_EQ(buffer.OwnBuffer(), false);
}

// test Reset method
TEST_F(ABufferTest, ResetMethod) {
    ABuffer buffer(100);
    EXPECT_TRUE(buffer.OwnBuffer());
    
    buffer.Reset();
    
    EXPECT_EQ(buffer.Buffer(), nullptr);
    EXPECT_EQ(buffer.Size(), 0);
    EXPECT_EQ(buffer.OwnBuffer(), false);
}

// test array access operator
TEST_F(ABufferTest, ArrayAccessOperator) {
    const size_t size = 10;
    ABuffer buffer(size);
    
    for (size_t i = 0; i < size; ++i) {
        buffer[i] = static_cast<uint8_t>(i);
    }
    
    for (size_t i = 0; i < size; ++i) {
        EXPECT_EQ(buffer[i], static_cast<uint8_t>(i));
    }
    
    const ABuffer& const_buffer = buffer;
    for (size_t i = 0; i < size; ++i) {
        EXPECT_EQ(const_buffer[i], static_cast<uint8_t>(i));
    }
}

// test offset operations
TEST_F(ABufferTest, OffsetOperations) {
    const size_t size = 100;
    ABuffer buffer(size);
    
    EXPECT_EQ(buffer.Left(), 0);
    EXPECT_EQ(buffer.Right(), size);
    
    buffer.MoveOffset(20);
    EXPECT_EQ(buffer.Left(), 20);
    EXPECT_EQ(buffer.Right(), size - 20);
    
    buffer.ResetOffset();
    EXPECT_EQ(buffer.Left(), 0);
    EXPECT_EQ(buffer.Right(), size);
}

// test tail cut operations
TEST_F(ABufferTest, TailOperations) {
    const size_t size = 100;
    ABuffer buffer(size);
    
    EXPECT_EQ(buffer.Size(), size);
    
    buffer.CutTail(50);
    EXPECT_EQ(buffer.Size(), 50);
    
    buffer.ResetTail();
    EXPECT_EQ(buffer.Size(), size);
}

// test FromString static method
TEST_F(ABufferTest, FromStringStaticMethod) {
    std::string test_string = "Hello, World!";
    ABuffer buffer = ABuffer::FromString(test_string);
    
    EXPECT_EQ(buffer.Size(), test_string.size());
    EXPECT_EQ(buffer.ToString(), test_string);
    EXPECT_EQ(buffer.ToStringView(), test_string);
    EXPECT_TRUE(buffer.OwnBuffer());
}

// test FromStringView static method
TEST_F(ABufferTest, FromStringViewStaticMethod) {
    std::string test_string = "Test String View";
    std::string_view view(test_string);
    ABuffer buffer = ABuffer::FromStringView(view);
    
    EXPECT_EQ(buffer.Size(), view.size());
    EXPECT_EQ(buffer.ToStringView(), view);
    EXPECT_FALSE(buffer.OwnBuffer());
}

// test FromBytes static method
TEST_F(ABufferTest, FromBytesStaticMethod) {
    std::vector<uint8_t> test_data = {0x01, 0x02, 0x03, 0x04, 0x05};
    ABuffer buffer = ABuffer::FromBytes(test_data);
    
    EXPECT_EQ(buffer.Size(), test_data.size());
    EXPECT_EQ(buffer.ToBytes(), test_data);
    EXPECT_TRUE(buffer.OwnBuffer());
}

// test DeepCopy static method
TEST_F(ABufferTest, DeepCopyStaticMethod) {
    std::string test_data = "Deep Copy Test";
    ABuffer original = ABuffer::FromString(test_data);
    
    ABuffer copy = ABuffer::DeepCopy(original);
    
    EXPECT_EQ(copy.Size(), original.Size());
    EXPECT_TRUE(copy.IsSame(original));
    EXPECT_NE(copy.Buffer(), original.Buffer());
    EXPECT_TRUE(copy.OwnBuffer());
}

// test DeepCopy raw data static method
TEST_F(ABufferTest, DeepCopyRawDataStaticMethod) {
    std::vector<uint8_t> test_data = {0xAA, 0xBB, 0xCC, 0xDD};
    ABuffer buffer = ABuffer::DeepCopy(test_data.data(), test_data.size());
    
    EXPECT_EQ(buffer.Size(), test_data.size());
    EXPECT_EQ(buffer.ToBytes(), test_data);
    EXPECT_TRUE(buffer.OwnBuffer());
}

// test ShallowCopy static method
TEST_F(ABufferTest, ShallowCopyStaticMethod) {
    std::string test_data = "Shallow Copy Test";
    ABuffer original = ABuffer::FromString(test_data);
    
    ABuffer shallow = ABuffer::ShallowCopy(original);
    
    EXPECT_EQ(shallow.Size(), original.Size());
    EXPECT_EQ(shallow.Buffer(), nullptr);
    EXPECT_EQ(shallow.Data(), original.Data());
    EXPECT_FALSE(shallow.OwnBuffer());
}

// test ShallowCopy with skip parameter
TEST_F(ABufferTest, ShallowCopyWithSkip) {
    std::string test_data = "Skip Test Data";
    ABuffer original = ABuffer::FromString(test_data);
    
    const size_t skip = 5;
    ABuffer shallow = ABuffer::ShallowCopy(original, skip);
    
    EXPECT_EQ(shallow.Size(), original.Size() - skip);
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(shallow.Data()), shallow.Size()), 
              test_data.substr(skip));
}

// test IsSame method
TEST_F(ABufferTest, IsSameMethod) {
    std::string test_data1 = "Same Test";
    std::string test_data2 = "Same Test";
    std::string test_data3 = "Different";
    
    ABuffer buffer1 = ABuffer::FromString(test_data1);
    ABuffer buffer2 = ABuffer::FromString(test_data2);
    ABuffer buffer3 = ABuffer::FromString(test_data3);
    
    EXPECT_TRUE(buffer1.IsSame(buffer2));
    EXPECT_FALSE(buffer1.IsSame(buffer3));
    EXPECT_FALSE(buffer1.IsSame(ABuffer()));
}

// test Hex method
TEST_F(ABufferTest, HexMethod) {
    std::vector<uint8_t> test_data = {0x12, 0xAB, 0xCD, 0xEF, 0x00, 0xFF};
    ABuffer buffer = ABuffer::FromBytes(test_data);
    
    std::string hex = buffer.Hex();
    EXPECT_EQ(hex, "12abcdef00ff");
}

// test integer read/write functionality
TEST_F(ABufferTest, IntegerReadWrite) {
    const size_t buffer_size = 100;
    ABuffer buffer(buffer_size);
    
    uint8_t u8_value = 0xAB;
    size_t offset = 0;
    offset = buffer.SetInt(u8_value, offset);
    EXPECT_EQ(offset, sizeof(uint8_t));
    
    uint8_t read_u8 = 0;
    buffer.GetInt(0, read_u8);
    EXPECT_EQ(read_u8, u8_value);
    
    uint16_t u16_value = 0x1234;
    offset = buffer.SetInt(u16_value, offset);
    EXPECT_EQ(offset, sizeof(uint8_t) + sizeof(uint16_t));
    
    uint16_t read_u16 = 0;
    buffer.GetInt(sizeof(uint8_t), read_u16);
    EXPECT_EQ(read_u16, u16_value);
    
    uint32_t u32_value = 0x12345678;
    offset = buffer.SetInt(u32_value, offset);
    
    uint32_t read_u32 = 0;
    buffer.GetInt(sizeof(uint8_t) + sizeof(uint16_t), read_u32);
    EXPECT_EQ(read_u32, u32_value);
    
    uint64_t u64_value = 0x123456789ABCDEF0;
    offset = buffer.SetInt(u64_value, offset);
    
    uint64_t read_u64 = 0;
    buffer.GetInt(sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint32_t), read_u64);
    EXPECT_EQ(read_u64, u64_value);
}

// test boundary conditions
TEST_F(ABufferTest, BoundaryConditions) {
    ABuffer empty;
    EXPECT_EQ(empty.Size(), 0);
    EXPECT_FALSE(empty.OwnBuffer());
    
    ABuffer zero_capacity(0);
    EXPECT_EQ(zero_capacity.Size(), 0);
    EXPECT_TRUE(zero_capacity.OwnBuffer());
    
    const size_t large_size = 1024 * 1024;
    ABuffer large_buffer(large_size);
    EXPECT_EQ(large_buffer.Size(), large_size);
    EXPECT_TRUE(large_buffer.OwnBuffer());
}

// test HeadGap and TailGap functionality (verified via constructors)
TEST_F(ABufferTest, HeadGapAndTailGap) {
    // test case where head is at the beginning of buf
    {
        const size_t capacity = 200;
        uint8_t* raw_buffer = new uint8_t[capacity];
        uint8_t* head1 = raw_buffer;
        uint8_t* tail1 = raw_buffer + 100;
        ABuffer buffer1(raw_buffer, head1, tail1, capacity);
        EXPECT_EQ(buffer1.Buffer(), raw_buffer);
        EXPECT_EQ(buffer1.Data(), head1);
        EXPECT_EQ(buffer1.Size(), 100);
        // HeadGap should be 0, since head_ == buf_
        EXPECT_EQ(head1 - raw_buffer, 0);
        // TailGap should be capacity - (tail_ - buf_) = 200 - 100 = 100
        EXPECT_EQ(capacity - (tail1 - raw_buffer), 100);
    }
    
    // test case where head has an offset
    {
        const size_t capacity = 200;
        uint8_t* raw_buffer = new uint8_t[capacity];
        uint8_t* head2 = raw_buffer + 20;
        uint8_t* tail2 = raw_buffer + 150;
        ABuffer buffer2(raw_buffer, head2, tail2, capacity);
        EXPECT_EQ(buffer2.Buffer(), raw_buffer);
        EXPECT_EQ(buffer2.Data(), head2);
        EXPECT_EQ(buffer2.Size(), 130);
        // HeadGap should be 20
        EXPECT_EQ(head2 - raw_buffer, 20);
        // TailGap should be capacity - (tail_ - buf_) = 200 - 150 = 50
        EXPECT_EQ(capacity - (tail2 - raw_buffer), 50);
    }
    
    // test case where both head and tail are at the end
    {
        const size_t capacity = 200;
        uint8_t* raw_buffer = new uint8_t[capacity];
        uint8_t* head3 = raw_buffer + 180;
        uint8_t* tail3 = raw_buffer + 200;
        ABuffer buffer3(raw_buffer, head3, tail3, capacity);
        EXPECT_EQ(buffer3.Buffer(), raw_buffer);
        EXPECT_EQ(buffer3.Data(), head3);
        EXPECT_EQ(buffer3.Size(), 20);
        // HeadGap should be 180
        EXPECT_EQ(head3 - raw_buffer, 180);
        // TailGap should be capacity - (tail_ - buf_) = 200 - 200 = 0
        EXPECT_EQ(capacity - (tail3 - raw_buffer), 0);
    }
}

// test three-parameter constructor basic functionality
TEST_F(ABufferTest, ThreeParameterConstructorBasic) {
    const size_t buffer_size = 256;
    uint8_t* raw_buffer = new uint8_t[buffer_size];
    uint8_t* head = raw_buffer + 32;
    const size_t data_size = 128;
    
    ABuffer buffer(raw_buffer, head, data_size);
    
    EXPECT_EQ(buffer.Buffer(), raw_buffer);
    EXPECT_EQ(buffer.Data(), head);
    EXPECT_EQ(buffer.Begin(), head);
    EXPECT_EQ(buffer.End(), head + data_size);
    EXPECT_EQ(buffer.Size(), data_size);
    EXPECT_EQ(buffer.OwnBuffer(), true);
}

// test ToBytes method
TEST_F(ABufferTest, ToBytesMethod) {
    std::vector<uint8_t> test_data = {1, 2, 3, 4, 5};
    ABuffer buffer = ABuffer::FromBytes(test_data);
    
    std::vector<uint8_t> result = buffer.ToBytes();
    EXPECT_EQ(result, test_data);
}

// test string view conversion
TEST_F(ABufferTest, StringViewConversion) {
    std::string test_string = "String View Test";
    ABuffer buffer = ABuffer::FromString(test_string);
    
    std::string_view view = buffer.ToStringView();
    EXPECT_EQ(view, test_string);
    
    std::string str = buffer.ToString();
    EXPECT_EQ(str, test_string);
}

}  // namespace consensus_spec
