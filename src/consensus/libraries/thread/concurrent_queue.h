// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <queue>
#include <vector>

#include "consensus/libraries/thread/lock.h"

namespace consensus_spec {

// A naive thread-safe queue that wraps std::queue.
template <class T, class Container = std::deque<T> >
class ConcurrentQueue {
  public:
    ConcurrentQueue() = default;
    ~ConcurrentQueue() = default;
    ConcurrentQueue(const ConcurrentQueue&) = delete;
    ConcurrentQueue& operator=(const ConcurrentQueue&) = delete;

  public:
    bool Empty() {
        UniqueGuard lock(mutex_);
        return queue_.empty();
    }

    size_t Size() {
        UniqueGuard lock(mutex_);
        return queue_.size();
    }

    void Push(const T& value) {
        UniqueGuard lock(mutex_);
        queue_.push(value);
    }

    bool Pop(T& popped_value) {
        UniqueGuard lock(mutex_);
        if (queue_.empty()) {
            return false;
        } else {
            popped_value = queue_.front();
            queue_.pop();
            return true;
        }
    }

    bool Peek(T& peeked_value) {
        UniqueGuard lock(mutex_);
        if (queue_.empty()) {
            return false;
        } else {
            peeked_value = queue_.front();
            return true;
        }
    }

    // bool Top(T &top_value, std::function<bool(const T &top_val)> f)

  private:
    std::queue<T, Container> queue_;
    Mutex mutex_;
};

}  // namespace consensus_spec
