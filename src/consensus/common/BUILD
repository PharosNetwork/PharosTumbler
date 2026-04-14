load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]),
    copts = COPTS,
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    deps = [
        "//src",
        "//src/consensus/libraries/common",
        "@pamir_framework//pamir/cetina",
        "@cobre//src/cobre/libraries/log",
        "@cobre//src/cobre/libraries/common:macro",
    ],
)
