// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/thread/worker.h"
#include "consensus/libraries/thread/thread_utils.h"
#include "consensus/libraries/log/consensus_log.h"

using namespace std;  // NOLINT(build/namespaces)

namespace consensus_spec {

Worker::Worker(const std::string& name, unsigned idle)
        : name_(name), idle_(idle), timer_(io_ctx_) {}

Worker::~Worker() {
    StopWorking();
}

string Worker::GetName() {
    return name_;
}

void Worker::SetName(const std::string& name) {
    if (!IsWorking()) {
        name_ = name;
    }
}

void Worker::SetIdle(unsigned idle) {
    idle_ = idle;
}

WorkerState Worker::GetState() {
    UniqueGuard lock(lock_);
    return state_;
}

unsigned Worker::GetIdle() {
    return idle_;
}

std::thread::id Worker::GetThreadId() {
    UniqueGuard lock(lock_);

    if (thread_) {
        return thread_->get_id();
    }

    return std::thread::id();
}

void Worker::StartWorking() {
    UniqueGuard lock(lock_);

    if (thread_) {
        return;
    }

    state_.exchange(WorkerState::Starting);
    condition_.notify_all();

    thread_.reset(new thread([&]() {
        thread_utils::SetCurrentThreadName(name_.c_str());
        {
            UniqueGuard lock(lock_);
            state_.exchange(WorkerState::Started);
            condition_.notify_all();
        }

        try {
            OnStarted();

            asio::executor_work_guard<asio::io_context::executor_type> work =
                asio::make_work_guard(io_ctx_);

            // OneLoop();

            timer_.ExpiresAt(std::chrono::steady_clock::now() + chrono::milliseconds(idle_));
            timer_.AsyncWait([this](const asio::error_code&) {
                this->OneLoop();
            });

            io_ctx_.run();

            work.reset();

            OnStopped();
        } catch (std::exception const& _e) {
            // do nothing for exception
            CS_LOG_ERROR("worker thread stop. exception:{}", _e.what());
        }

        {
            UniqueGuard lock(lock_);
            state_.exchange(WorkerState::Stopped);
            condition_.notify_all();
        }
    }));
}

void Worker::StopWorking() {
    {
        UniqueGuard lock(lock_);
        if (!thread_) {
            return;
        }
    }

    {
        UniqueGuard lock(lock_);
        while (state_ != WorkerState::Started) {
            condition_.wait(lock);
        }

        state_.exchange(WorkerState::Stopping);
        condition_.notify_all();

        io_ctx_.stop();

        while (!io_ctx_.stopped() || state_ != WorkerState::Stopped) {
            condition_.wait(lock);
        }
    }

    thread_->join();
    thread_.reset();
}

bool Worker::IsWorking() {
    UniqueGuard lock(lock_);

    return state_ == WorkerState::Started;
}

asio::io_context& Worker::GetIoContext() {
    return io_ctx_;
}

void Worker::OneLoop() {
    DoWork();

    timer_.ExpiresAt(std::chrono::steady_clock::now() + chrono::milliseconds(idle_));
    timer_.AsyncWait([this](const asio::error_code&) {
        this->OneLoop();
    });
}

}  // namespace consensus_spec
