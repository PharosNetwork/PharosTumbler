// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include "consensus/libraries/thread/worker.h"

// #include "consensus/libraries/common/conversion.h"

namespace consensus_spec {

class MyWorker : public Worker {
  public:
    virtual void OnStarted() {
        test_counter_++;
    }

    virtual void DoWork() {
        if (flag_) {
            work_counter_++;
            flag_ = false;
        }
    }

    virtual void OnStopped() {
        test_counter_--;
    }

  public:
    int test_counter_ = 0;
    bool flag_ = false;
    int work_counter_ = 0;
    int task_count_ = 0;
};

class ThreadWorkerTest : public testing::Test {};

TEST_F(ThreadWorkerTest, WorkerNameTest) {
    MyWorker worker;
    EXPECT_EQ(worker.GetName(), "worker");

    worker.SetName("myworker");
    EXPECT_EQ(worker.GetName(), "myworker");
}

TEST_F(ThreadWorkerTest, WorkerIdleTest) {
    MyWorker worker;
    EXPECT_EQ(worker.GetIdle(), 10);

    worker.SetIdle(20);
    EXPECT_EQ(worker.GetIdle(), 20);
}

TEST_F(ThreadWorkerTest, WorkerStateTest) {
    MyWorker worker;
    EXPECT_TRUE(!worker.IsWorking());
    EXPECT_EQ(worker.GetState(), WorkerState::Starting);
    EXPECT_EQ(worker.test_counter_, 0);

    worker.StartWorking();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(worker.IsWorking());
    EXPECT_EQ(worker.test_counter_, 1);
    EXPECT_TRUE(worker.GetThreadId() != std::this_thread::get_id());

    worker.StopWorking();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(!worker.IsWorking());
    EXPECT_EQ(worker.GetState(), WorkerState::Stopped);
    EXPECT_EQ(worker.test_counter_, 0);
}

TEST_F(ThreadWorkerTest, WorkerTaskTest) {
    MyWorker worker;
    worker.StartWorking();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(!worker.flag_);
    EXPECT_EQ(worker.work_counter_, 0);

    worker.flag_ = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(!worker.flag_);
    EXPECT_EQ(worker.work_counter_, 1);

    worker.flag_ = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_TRUE(!worker.flag_);
    EXPECT_EQ(worker.work_counter_, 2);

    int sum = 0;
    for (int i = 0; i < 2; ++i) {
        worker.PushTask([&]() {
            worker.task_count_ += i;
        });
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        sum += i;
        EXPECT_EQ(worker.task_count_, sum);
    }

    worker.StopWorking();
}

}  // namespace consensus_spec