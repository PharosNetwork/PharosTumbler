// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <asio.hpp>
#include "consensus/libraries/thread/lock.h"
#include "consensus/libraries/utils/timer.h"

namespace consensus_spec {

enum class WorkerState {
    Starting,
    Started,
    Stopping,
    Stopped
};

class Worker {
  public:
    explicit Worker(const std::string& name = "worker", unsigned idle = 10);
    virtual ~Worker();

  public:
    std::string GetName();
    void SetName(const std::string& name);
    void SetIdle(unsigned idle);
    virtual WorkerState GetState();
    unsigned GetIdle();
    std::thread::id GetThreadId();

    virtual void StartWorking();
    virtual void StopWorking();
    bool IsWorking();

    template <typename T>
    void PushTask(T&& handler) {
        io_ctx_.post(std::forward<T>(handler));
    }

    asio::io_context& GetIoContext();

  protected:
    virtual void OnStarted() {}

    virtual void DoWork() {}

    virtual void DoTask() {}

    virtual void OnStopped() {}

  private:
    void OneLoop();

  private:
    std::string name_;                     // Worker name
    unsigned idle_;                        // Waite time on idle, unit is millseconds
    Mutex lock_;                           // Thread lock
    Condition condition_;                  // Notification when state changes
    std::unique_ptr<std::thread> thread_;  // The work thread
    std::atomic<WorkerState> state_ = {WorkerState::Starting};  // Worker state
    asio::io_context io_ctx_;                                   // The event driver

    Timer timer_;
};

}  // namespace consensus_spec
