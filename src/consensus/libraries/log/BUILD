load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "log",
    srcs = glob(["*.cpp"], allow_empty = True),
    hdrs = glob(["*.h"]),
    deps = [
        "@cobre//src/cobre/libraries/log:log",
    ],
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    linkstatic = 1,
    visibility = ["//visibility:public"],
)
