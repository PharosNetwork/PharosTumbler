load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "time_utils",
    srcs = glob(["time_utils.cpp"]),
    hdrs = glob([
        "time_utils.h",
    ]),
    copts = COPTS,
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    deps = [
        "//src",
    ],
)

cc_library(
    name = "timer",
    srcs = ["timer.cpp"],
    hdrs = ["timer.h"],
    copts = COPTS,
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    deps = [
        "//src",
        "//src/consensus/libraries/common:type_define",
        "@asio_1_22_1//:asio",
    ],
)
