load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library")
load("//bazel:variables.bzl", "BINARY_LINKOPTS", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")
load("@com_grail_bazel_compdb//:aspects.bzl", "compilation_database")

cc_binary(
    name = "test_consensus",
    srcs = glob(["test_helper/test_consensus.cpp", "protocol/*.h", "protocol/*.cpp", "libraries/*.cpp"]),
    copts = COPTS + ["-fno-access-control"] + ["-g", "-O0"],
    linkopts = BINARY_LINKOPTS + ["-pthread"] + ["-lstdc++fs"],
    linkstatic = True,
    deps = [
        "@PLACEHOLDER_cpp_thirdparty_googletest_1_10_0//:gtest",
        "//src/consensus/libraries/thread:multithread",
        "//src/consensus/libraries/wal:wal",
        "//src/consensus/protocol:mytumbler",
    ],
    visibility = ["//visibility:public"]
)

compilation_database(
        name = "example_compdb",
        targets = [
                "//test:test_consensus"
        ],
        # [Optional]
        # If your exec root (value returned by `bazel info execution_root`)
        # is constant across your users, then you can supply the value here.
        # Otherwise, the default is `__EXEC_ROOT__` which you can replace in
        # the output file using `sed` or similar tool (see below).
        exec_root = "__EXEC_ROOT__",
)
