// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/common/a_buffer.h"

#include <cstring>
#include <string>
#include <string_view>
#include <utility>

namespace consensus_spec {

ABuffer::ABuffer() noexcept
        : buf_(nullptr), head_(nullptr), tail_(nullptr), offset_(nullptr), capacity_(0) {}

ABuffer::ABuffer(size_t capacity) noexcept
        : buf_(new uint8_t[capacity]),
          head_(buf_),
          tail_(buf_ + capacity),
          offset_(head_),
          capacity_(capacity) {}

ABuffer::ABuffer(uint8_t* buf, size_t sz) noexcept
        : buf_(buf), head_(buf), tail_(buf + sz), offset_(buf), capacity_(sz) {}

ABuffer::ABuffer(uint8_t* buf, uint8_t* head, uint8_t* tail, size_t capacity) noexcept
        : buf_(buf), head_(head), tail_(tail), offset_(head), capacity_(capacity) {}

ABuffer::ABuffer(uint8_t* buf, uint8_t* head, size_t sz) noexcept
        : buf_(buf), head_(head), tail_(head + sz), offset_(head), capacity_(sz) {}

// move
ABuffer::ABuffer(ABuffer&& other) noexcept
        : buf_(other.buf_),
          head_(other.head_),
          tail_(other.tail_),
          offset_(other.offset_),
          capacity_(other.capacity_) {
    other.buf_ = nullptr;
    other.head_ = nullptr;
    other.tail_ = nullptr;
    other.offset_ = nullptr;
    other.capacity_ = 0;
}

ABuffer& ABuffer::operator=(ABuffer&& other) noexcept {
    std::swap(buf_, other.buf_);
    std::swap(head_, other.head_);
    std::swap(tail_, other.tail_);
    std::swap(offset_, other.offset_);
    std::swap(capacity_, other.capacity_);
    return *this;
}

ABuffer::~ABuffer() {
    Free();
}

void ABuffer::Reset() noexcept {
    buf_ = nullptr;
    head_ = nullptr;
    tail_ = nullptr;
    offset_ = nullptr;
    capacity_ = 0;
}

void ABuffer::Free() noexcept {
    if (buf_) {
        delete[] buf_;
    }
    Reset();
}

// conversion
ABuffer ABuffer::FromBytes(const std::vector<uint8_t>& vec) {
    size_t size = vec.size();
    auto* buf = new uint8_t[size];
    ::memcpy(buf, vec.data(), size);
    return ABuffer(buf, buf, buf + size, size);
}

ABuffer ABuffer::FromString(const std::string& str) {
    size_t size = str.size();
    auto* buf = new uint8_t[size];
    ::memcpy(buf, str.data(), size);
    return ABuffer(buf, buf, buf + size, size);
}

ABuffer ABuffer::FromStringView(std::string_view str) {
    size_t size = str.size();
    auto* head = reinterpret_cast<const uint8_t*>(str.data());
    auto* tail = reinterpret_cast<const uint8_t*>(str.data() + size);
    return ABuffer(nullptr, const_cast<uint8_t*>(head), const_cast<uint8_t*>(tail), size);
}

ABuffer ABuffer::DeepCopy(const ABuffer& other) {
    size_t size = other.Size();
    auto* buf = new uint8_t[size];
    ::memcpy(buf, other.Data(), size);
    return ABuffer(buf, buf, buf + size, size);
}

ABuffer ABuffer::DeepCopy(const uint8_t* buf, size_t sz) {
    auto* new_buf = new uint8_t[sz];
    ::memcpy(new_buf, buf, sz);
    return ABuffer(new_buf, sz);
}

ABuffer ABuffer::ShallowCopy(const ABuffer& other, size_t skip) {
    size_t size = other.Size();
    if (skip >= size) {
        return ABuffer();
    }
    size -= skip;
    auto* buf = other.Data() + skip;
    return ABuffer(nullptr, buf, buf + size, size);
}

uint8_t& ABuffer::operator[](size_t index) {
    assert(index < Size());
    return head_[index];
}

const uint8_t& ABuffer::operator[](size_t index) const {
    assert(index < Size());
    return head_[index];
}

void ABuffer::Swap(ABuffer& other) {
    std::swap(buf_, other.buf_);
    std::swap(head_, other.head_);
    std::swap(tail_, other.tail_);
    std::swap(offset_, other.offset_);
    std::swap(capacity_, other.capacity_);
}

std::string ABuffer::Hex() const {
    std::string hex;
    hex.reserve(capacity_ << 1);
    static const char* hexdigits = "0123456789abcdef";
    for (auto it = Begin(); it != End(); ++it) {
        auto h = (*it >> 4) & 0x0f;
        auto l = *it & 0x0f;
        hex.push_back(hexdigits[h]);
        hex.push_back(hexdigits[l]);
    }
    return hex;
}

}  // namespace consensus_spec
