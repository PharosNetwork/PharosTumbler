// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/utils/timer.h"

#include <cstddef>

namespace consensus_spec {

using IoContext = asio::io_context;
using Duration = asio::steady_timer::duration;
using TimePoint = asio::steady_timer::time_point;

Timer::Timer(IoContext& io_context) : timer_(io_context), io_ctx_(io_context) {
    // Do nothing.
}

void Timer::Wait() {
    timer_.wait();
}

void Timer::AsyncWait(std::function<void(const asio::error_code&)> handler) {
    timer_.async_wait(handler);
}

std::size_t Timer::ExpiresAfter(const Duration& expiry_time) {
    return timer_.expires_after(expiry_time);
}

std::size_t Timer::ExpiresAt(const TimePoint& expiry_time) {
    return timer_.expires_at(expiry_time);
}

TimePoint Timer::Expiry() const {
    return timer_.expiry();
}

IoContext& Timer::GetIoContext() {
    return io_ctx_;
}

std::size_t Timer::Cancel() {
    return timer_.cancel();
}

std::size_t Timer::CancelOne() {
    return timer_.cancel_one();
}

MyTimer::MyTimer(IoContext& io_context, uint32_t interval, timerCallback cb)
        : timer_(io_context),
          timeout_interval_(interval),
          timer_cancel_(false),
          cb_(std::move(cb)) {}

}  // namespace consensus_spec
