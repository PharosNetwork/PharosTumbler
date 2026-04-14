COPTS = select({
    "@consensus_spec//bazel:config_coverage": [
        "-g",
        "--coverage",
        "-fno-inline",
        '-fkeep-inline-functions',
        '-fkeep-static-functions',
    ],
    "//conditions:default": [],
}) + select({
    "@consensus_spec//bazel:config_memory_check": [
        "-fsanitize=address",
        "-fno-omit-frame-pointer",
    ],
    "//conditions:default": [],
}) + [
    # "-Werror",
    "-Wextra",
    "-Wall",
    "-Wpedantic",
    "-Wno-variadic-macros",
    "-Wno-vla",
    "-Wno-unused-result",
    "-Wno-unused-but-set-variable",
] + select({
    "@consensus_spec//bazel:osx": [
       "-Wno-gnu-zero-variadic-macro-arguments",
       "-Wno-format"
    ],
    "//conditions:default": [],
})


DEFINES = select({
    "@consensus_spec//bazel:disable_crash_dump": [
        "DISABLE_CRASH_DUMP=ON",
    ],
    "//conditions:default": [],
}) + select({
    "@consensus_spec//bazel:enable_failpoint": [
        "ENABLE_FAILPOINT=ON",
    ],
    "//conditions:default": [],
  }) + select({
      "@consensus_spec//bazel:enable_trace": [
          "ENABLE_TRACE=ON",
      ],
      "//conditions:default": [],
  }) + select({
    "@consensus_spec//bazel:enable_cubenet": [
        "ENABLE_CUBENET=ON",
    ],
    "//conditions:default": [],
}) + select ({
    "@consensus_spec//bazel:light": [
        "LIGHT",
    ],
    "//conditions:default": [],
}) + select({
    "@consensus_spec//bazel:avm_run": [
        "ENABLE_AVM_RUN=ON",
    ],
    "//conditions:default": [],
}) + select({
    "@consensus_spec//bazel:wamr_mode": [
        "ENABLE_WAMR=ON",
    ],
    "//conditions:default": [],
}) + select({
    "@consensus_spec//bazel:antvm_singlepass_mode": [
        "ENABLE_ANTVM_SINGLEPASS=ON",
    ],
    "//conditions:default": [],
})

# linkopts for cc_binary
BINARY_LINKOPTS = select({
    "@consensus_spec//bazel:config_coverage": [
        "--coverage",
        # "-Wl,--whole-archive",
    ],
    "//conditions:default": [],
}) + select({
    "@consensus_spec//bazel:config_memory_check": [
        "-fsanitize=address",
        "-static-libasan",
    ],
    "//conditions:default": [],
}) + [
    "-lm",
    "-ldl",
] + select({
    "@consensus_spec//bazel:osx": [
        "-lstdc++",
        "-stdlib=libc++",
        "-rdynamic"
    ],
    "//conditions:default": [
        "-static-libstdc++",
        "-static-libgcc",
        "-l:libatomic.a",
        "-lrt",
    ],
})


# linkopts for cc_library
LIBRARY_LINKOPTS = select({
    "@consensus_spec//bazel:config_coverage": [
        "--coverage",
    ],
    "//conditions:default": [],
})

DEP_TCMALLOC = select({
    "@consensus_spec//bazel:config_tcmalloc": [
        "@PLACEHOLDER_cpp_thirdparty_gperftools_2_7//:tcmalloc",
        "@PLACEHOLDER_cpp_thirdparty_libunwind_1_8_1//:libunwind",
    ],
    "//conditions:default": [],
})
