// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace consensus_spec {
using Mutex = std::mutex;
using RecursiveMutex = std::recursive_mutex;
using Condition = std::condition_variable;

using Guard = std::lock_guard<std::mutex>;
using ScopedLock = std::scoped_lock<std::mutex>;
using UniqueGuard = std::unique_lock<std::mutex>;
using RecursiveGuard = std::lock_guard<std::recursive_mutex>;
}  // namespace consensus_spec
