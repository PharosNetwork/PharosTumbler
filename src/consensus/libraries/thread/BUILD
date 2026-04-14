load("@rules_cc//cc:defs.bzl", "cc_library")
load("//bazel:variables.bzl", "COPTS", "DEFINES", "LIBRARY_LINKOPTS")

cc_library(
    name = "multithread",
    srcs = glob(["*.cpp"]),
    hdrs = glob(["*.h"]) + glob(["*.hpp"], allow_empty = True),
    copts = COPTS,
    linkopts = LIBRARY_LINKOPTS,
    local_defines = DEFINES,
    deps = [
        ":lock",
        ":concurrent_queue",
        ":worker"
	],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "lock",
    srcs = ["lock.h"],
    hdrs = ["lock.h"],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "concurrent_queue",
    srcs = ["concurrent_queue.h"],
    hdrs = ["concurrent_queue.h"],
    deps = [
        ":lock"
    ],
    visibility = ["//visibility:public"],
)

cc_library(
    name = "thread_utils",
    srcs = ["thread_utils.cpp"],
    hdrs = ["thread_utils.h"],
    deps = [
        "//src",
    ],
    copts = COPTS,
    local_defines = DEFINES,
    linkopts = LIBRARY_LINKOPTS + ["-lpthread"],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)

cc_library(
    name = "worker",
    srcs = ["worker.cpp"],
    hdrs = ["worker.h"],
    deps = [
        "@asio_1_22_1//:asio",
        "//src/consensus/libraries/utils:time_utils",
        "//src/consensus/libraries/utils:timer",
	    ":lock",
        ":thread_utils",
        "//src/consensus/libraries/log",
    ],
    copts = COPTS,
    local_defines = DEFINES,
    linkopts = LIBRARY_LINKOPTS + ["-lpthread"],
    linkstatic = 1,
    visibility = ["//visibility:public"],
)
