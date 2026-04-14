# config_settings

config_setting(
    name = "config_memory_check",
    values = {"define": "MEMORY_CHECK=ON"},
)

config_setting(
    name = "config_coverage",
    values = {"define": "COVERAGE=ON"},
)

config_setting(
    name = "disable_crash_dump",
    values = {"define": "DISABLE_CRASH_DUMP=ON"},
)

config_setting(
    name = "enable_failpoint",
    values = {"define": "ENABLE_FAILPOINT=ON"},
)

config_setting(
    name = "enable_trace",
    values = {"define": "ENABLE_TRACE=ON"},
)

# config_setting(
#     name = "enable_contract_fuzz",
#     values = {"define": "ENABLE_CONTRACT_FUZZ=ON"},
# )

config_setting(
    name = "release_mode",
    values = {"define": "RELEASE_BUILD=ON"},
)
config_setting(
    name = "config_tcmalloc",
    values = {"define": "tcmalloc=ON"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "light",
    values = {"define": "LIGHT=ON"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "enable_cubenet",
    values = {"define": "ENABLE_CUBENET=ON"},
)

config_setting(
    name = "avm_run",
    values = {"define": "AVM_RUN=ON"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "osx",
    constraint_values = [
        "@platforms//os:osx",
    ],
    visibility = ["//visibility:public"],
)

config_setting(
    name = "wamr_mode",
    values = {"define": "ENABLE_WAMR=ON"},
    visibility = ["//visibility:public"],
)

config_setting(
    name = "antvm_singlepass_mode",
    values = {"define": "ENABLE_ANTVM_SINGLEPASS=ON"},
    visibility = ["//visibility:public"],
)
