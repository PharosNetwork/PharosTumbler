// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <utility>  // _GLIBCXX_HAVE_ALIGNED_ALLOC
#include <cstddef>
#include <functional>

#include <asio.hpp>  // _GLIBCXX_HAVE_ALIGNED_ALLOC used in config.hpp which defined in bits/config.h, include asio.hpp after c++headers

namespace consensus_spec {

class Timer {
  public:
    Timer() = delete;
    explicit Timer(asio::io_context& io_context);

    /******************************************************************
     * Function    : Wait
     * Description : Perform a blocking wait on the timer.  Blocks and
     *               does not return until the timer has expired.
     * Input       : None.
     * Output      : None.
     * Return      : None.
     * Exception   : asio::system_error, thrown on failure.
     * Usage       :
     ******************************************************************/
    void Wait();

    /******************************************************************
     * Function    : AsyncWait
     * Description : Start an asynchronous wait on the timer.
     *               The function prototype the wait handler required is:
     *
     *               using WaitHandler =
     *                   void (*)(const asio::error_code&);
     *
     * Input       : @handler       - See description above.
     * Output      : None.
     * Return      : None.
     * Exception   : None.
     * Usage       :
     ******************************************************************/
    void AsyncWait(std::function<void(const asio::error_code&)> handler);

    /******************************************************************
     * Function    : ExpiresAfter
     * Description : Set the timer's expiry time relative to now.  Any
     *               pending asynchronous wait operation will be
     *               cancelled.
     * Input       : @expiry_time       - The expiry time to be used
     *                                    for the timer.
     * Output      : None.
     * Return      : The number of asynchronous operations that were
     *               cancelled.
     * Exception   : asio::system_error, thrown on failure.
     * Usage       :
     ******************************************************************/
    std::size_t ExpiresAfter(const asio::steady_timer::duration& expiry_time);

    /******************************************************************
     * Function    : ExpiresAt
     * Description : Set the timer's expiry time as an absolute time.
     *               Any pending asynchronous wait operation will be
     *               cancelled.
     * Input       : @expiry_time       - The expiry time to be used
     *                                    for the timer.
     * Output      : None.
     * Return      : The number of asynchronous operations that were
     *               cancelled.
     * Exception   : asio::system_error, thrown on failure.
     * Usage       :
     ******************************************************************/
    std::size_t ExpiresAt(const asio::steady_timer::time_point& expiry_time);

    /******************************************************************
     * Function    : Expiry
     * Description : Get the timer's expiry time as an absolute time.
     * Input       : None.
     * Output      : None.
     * Return      : The timer's expiry time as an absolute time.
     * Exception   : None.
     * Usage       :
     ******************************************************************/
    asio::steady_timer::time_point Expiry() const;

    /******************************************************************
     * Function    : GetIoContext
     * Description : Get the IoContext object associated with the
     *               timer.
     * Input       : None.
     * Output      : None.
     * Return      : A reference to the IoContext object associated
     *               with the timer.
     * Exception   : None.
     * Usage       :
     ******************************************************************/
    asio::io_context& GetIoContext();

    /******************************************************************
     * Function    : Cancel
     * Description : Forces the completion of any pending asynchronous
     *               wait operation against the timer.
     * Input       : None.
     * Output      : None.
     * Return      : The number of asynchronous operations that were
     *               cancelled.
     * Exception   : asio::system_error, thrown on failure.
     * Usage       :
     ******************************************************************/
    std::size_t Cancel();

    /******************************************************************
     * Function    : CancelOne
     * Description : Forces the completion of one pending asynchronous
     *               wait operation against the timer.  Handlers are
     *               cancelled in FIFO order.
     * Input       : None.
     * Output      : None.
     * Return      : The number of asynchronous operations that were
     *               cancelled, either 0 or 1.
     * Exception   : asio::system_error, thrown on failure.
     * Usage       :
     ******************************************************************/
    std::size_t CancelOne();

  private:
    asio::steady_timer timer_;
    asio::io_context& io_ctx_;
};

using timerCallback = std::function<void(const asio::error_code& code, bool timer_cancel)>;

class MyTimer {
  public:
    MyTimer(asio::io_context& io_context, uint32_t interval, timerCallback cb);
    ~MyTimer() = default;

    inline void Start() {
        timer_cancel_.store(false);
        timer_.ExpiresAfter(std::chrono::milliseconds(timeout_interval_));
        timer_.AsyncWait([this](const asio::error_code& code) {
            this->Callback(code);
        });
    }

    inline void Reset() {
        timer_.Cancel();
        Start();
    }

    inline void Stop() {
        timer_cancel_.store(true);
        timer_.Cancel();
    }

    inline void ResetTimeoutInterval(uint32_t interval) {
        timeout_interval_ = interval;
    }

    inline uint32_t GetTimeoutInterval() {
        return timeout_interval_;
    }

  private:
    inline void Callback(const asio::error_code& code) {
        cb_(code, timer_cancel_.load());
    }

  private:
    Timer timer_;
    uint32_t timeout_interval_;  // ms
    std::atomic<bool> timer_cancel_;
    timerCallback cb_ = nullptr;
};

}  // namespace consensus_spec
