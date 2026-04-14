// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

// global env
#include "gtest/gtest.h"
#include "cobre/libraries/log/logging.h"

int print_console = 0;

class ConsensusEnv : public testing::Environment {
  public:
    virtual void SetUp() {
        if (print_console) {
            printf("init console log\n");
            INIT_CONSOLE_LOG();
        }
    }

    virtual void TearDown() {
        if (print_console) {
            printf("destroy console log\n");
            DESTROY_LOG();
        }
    }
};

static const char* g_json_log_conf = R"({
    "consensus": {
      "filename": "log/consensus.log",
      "max_file_size": 20971520,
      "max_files": 100,
      "level": "debug",
      "flush": true
    }
}
)";

GTEST_API_ int main(int argc, char** argv) {
    // for random port
    int random = time(NULL) + getpid();
    srand(random);
    // parse arguments
    testing::InitGoogleTest(&argc, argv);
    if (argc >= 2 && std::string(argv[1]) == "--enable_print_console=true") {
        print_console = 1;
    }
    if (!print_console) {
        // init logger
        INIT_LOG_WITH_JSON(g_json_log_conf);
    }
    // VtypeCodec::UseVtypeCodec(true);
    // run all test cases
    testing::AddGlobalTestEnvironment(new ConsensusEnv());
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    return RUN_ALL_TESTS();
}