load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "mytumbler",
    srcs = glob(["*.cpp"]),
    deps = [
        "//src/consensus/libraries/common",
        "//src/consensus/schema:consensus_cc_ssz",
        "//src/consensus/libraries/utils:time_utils",
        "//src/consensus/libraries/thread:multithread",
        "//src/consensus/libraries/log:log",
        "@pamir_framework//pamir/cetina",
        "//src/consensus/common",
        "//src/consensus/libraries/wal:wal",
    ],
    copts = COPTS,
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    hdrs = glob(["*.h"]),
)
