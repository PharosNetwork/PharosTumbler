// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string.h>

#include <cassert>  // for assert
#include <string>
#include <string_view>
#include <vector>
#include <cstdint>

namespace consensus_spec {
// ABuffer used in
//      1. network io
//      2. storage io
//      3. buf of ssz object
// can only be initialized once
// supports move
// does not support resize
// supports a write pointer for multiple send and receive operations
// does not support inheritance, saving 8 bytes of vtable pointer space
class ABuffer final /* no inheritance */ {
  public:
    ABuffer() noexcept;

    // alloc buffer size `capacity` and own it
    explicit ABuffer(size_t capacity) noexcept;

    // give the ownership of buf to ABuffer
    ABuffer(uint8_t* buf, size_t sz) noexcept;

    // 1. if buf != nullptr, give the ownership of buf to ABuffer
    // 2. if buf == nullptr, ABuffer does not own the buf, behaves like string_view
    // 3. capacity is the size of the buf
    // 4. realdata is [head, tail)
    ABuffer(uint8_t* buf, uint8_t* head, uint8_t* tail, size_t capacity) noexcept;
    ABuffer(uint8_t* buf, uint8_t* head, size_t sz) noexcept;

    // no copy, call `DeepCopy` if really need
    ABuffer(const ABuffer& other) = delete;
    ABuffer& operator=(const ABuffer& other) = delete;

    // support move
    ABuffer(ABuffer&& other) noexcept;
    ABuffer& operator=(ABuffer&& other) noexcept;

    ~ABuffer();

    uint8_t& operator[](size_t index);
    const uint8_t& operator[](size_t index) const;

    // free if own the buf_(i.e. buf_ != nullptr)
    void Free() noexcept;
    void Reset() noexcept;
    void Swap(ABuffer& other);

    // for io read/write
    void MoveOffset(size_t sz) noexcept {
        assert(Right() >= sz);
        offset_ += sz;
    };

    inline void ResetOffset() noexcept {
        offset_ = head_;
    };

    inline void CutTail(size_t sz) noexcept {
        assert(Left() <= sz);
        tail_ = head_ + sz;
    };

    inline void ResetTail() noexcept {
        tail_ = head_ + capacity_;
    };

    inline uint8_t* Begin() const noexcept {
        return head_;
    }

    inline uint8_t* End() const noexcept {
        return tail_;
    }

    inline size_t Left() noexcept {
        return static_cast<size_t>(offset_ - head_);
    }

    inline size_t Right() noexcept {
        return static_cast<size_t>(tail_ - offset_);
    }

    inline uint8_t* Buffer() const {
        return buf_;
    };

    inline uint8_t* Data() const {
        return head_;
    };

    inline uint8_t* Offset() const {
        return offset_;
    };

    inline size_t Size() const {
        return tail_ - head_;
    };

    inline std::string_view ToStringView() const {
        return std::string_view(reinterpret_cast<const char*>(Begin()), Size());
    }

    inline std::string ToString() const {
        return std::string(reinterpret_cast<const char*>(Begin()), Size());
    }

    inline std::vector<uint8_t> ToBytes() const {
        return std::vector<uint8_t>(Begin(), End());
    }

    inline bool IsSame(const ABuffer& buffer) const {
        return (Size() == buffer.Size() ? (memcmp(Data(), buffer.Data(), Size()) == 0) : false);
    }

    inline bool OwnBuffer() const {
        return buf_ != nullptr;
    }

    template <typename T>
    size_t SetInt(T t, size_t offset);
    template <typename T>
    size_t GetInt(size_t offset, T& t) const;

    static ABuffer FromString(const std::string& str);
    static ABuffer FromStringView(std::string_view str);

    static ABuffer FromBytes(const std::vector<uint8_t>& vec);

    // copy the data [other.head, other.tail) to this ABuffer
    static ABuffer DeepCopy(const ABuffer& other);
    static ABuffer DeepCopy(const uint8_t* buf, size_t sz);

    static ABuffer ShallowCopy(const ABuffer& other, size_t skip = 0);

    std::string Hex() const;

  private:
    uint8_t* buf_;  // maintain and own the buf

    uint8_t* head_;    // the beginning of the real data
    uint8_t* tail_;    // the end of the real data
    uint8_t* offset_;  // read or write index

    size_t capacity_;  // the length of buf
};

template <typename T>
size_t ABuffer::SetInt(T t, size_t offset) {
    assert(std::is_integral<T>());
    assert(offset + sizeof(T) <= Size());
    for (size_t i = 0; i < sizeof(T); ++i) {
        int shift = 8 * (sizeof(T) - i - 1);
        head_[offset + i] = ((t >> shift) & 0xff);
    }
    return offset + sizeof(T);
}

template <typename T>
size_t ABuffer::GetInt(size_t offset, T& t) const {
    assert(offset + sizeof(T) <= Size());
    t = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        int shift = 8 * (sizeof(T) - i - 1);
        t = t | (((T)(head_[offset + i])) << shift);  // NOLINT
    }
    return offset + sizeof(T);
}

}  // namespace consensus_spec
