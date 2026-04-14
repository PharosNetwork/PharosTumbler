load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "wal",
    srcs = glob(["*.h","*.cpp"]),
    hdrs = glob(["*.h"]),
    deps = [
        "//src/consensus/common",
        "//src/consensus/libraries/common",
        "@pamir_framework//pamir/cetina",
        "@cobre//src/cobre/libraries/common:macro",
        "@PLACEHOLDER_cpp_thirdparty_zlib_1_2_11//:main"
    ],
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    linkstatic = 1,
    visibility = ["//visibility:public"],
)
