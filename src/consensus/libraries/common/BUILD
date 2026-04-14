load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "common",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]) + glob(["*.hpp"], allow_empty = True),
    copts = COPTS,
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    deps = [
        "//src",
        "//src/consensus/libraries/thread:lock",
        "//src/consensus/libraries/log",
        "//src/consensus/libraries/utils:time_utils",
	],
)

cc_library(
    name = "type_define",
    hdrs = ["type_define.h"],
)
