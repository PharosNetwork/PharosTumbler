load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

load("//third_party/asio/1.22.1:asio.bzl", "asio_1_22_1_deps")
load("@PLACEHOLDER_cpp_thirdparty//common:defs.bzl", "foreign_cc_deps")
load("@PLACEHOLDER_cpp_thirdparty//tongsuo/8.4.0:tongsuo.bzl", "tongsuo_8_4_0_deps")
load("@pamir_framework//third_party:kuafu.bzl", "kuafu_deps")
load("@rules_foreign_cc//foreign_cc:repositories.bzl", "rules_foreign_cc_dependencies")
load("//third_party/cppssz:cppssz.bzl", "cppssz_deps")

def _third_party_deps_impl(_ctx):
    asio_1_22_1_deps()
    foreign_cc_deps()
    rules_foreign_cc_dependencies(
        register_default_tools = False,
        register_preinstalled_tools = False,
        register_built_tools = False,
        register_toolchains = False,
    )

    # spdlog: use PLACEHOLDER_cpp_thirdparty_spdlog_1_7_0 from @cobre//third_party:deps.bzl (MODULE.bazel).

    # PLACEHOLDER_cpp_thirdparty_gtest_1_8_1: from pamir_framework third_party_deps extension.

    http_archive(
        name = "PLACEHOLDER_cpp_thirdparty_googletest_1_10_0",
        sha256 = "PLACEHOLDER",
        strip_prefix = "googletest-release-1.10.0",
        urls = [
            # TODO: replace with public mirror URL for googletest release-1.10.0.tar.gz
            "https://github.com/google/googletest/archive/release-1.10.0.zip",
        ],
        patch_cmds = [
            """sed -i 's|@bazel_tools//platforms:|@platforms//os:|g' BUILD.bazel""",
            """sed -i 's|@bazel_tools//platforms:windows|@platforms//os:windows|g' BUILD.bazel""",
            """sed -i 's|@bazel_tools//platforms:osx|@platforms//os:macos|g' BUILD.bazel""",
            """sed -i 's|@bazel_tools//platforms:linux|@platforms//os:linux|g' BUILD.bazel""",
        ],
    )

    http_archive(
        name = "com_grail_bazel_compdb",
        sha256 = "PLACEHOLDER",
        strip_prefix = "bazel-compilation-database-master",
        urls = [
            # TODO: replace with public mirror URL for bazel-compilation-database
        ],
    )

    tongsuo_8_4_0_deps()
    kuafu_deps()

    cppssz_deps()

third_party_deps = module_extension(
    implementation = _third_party_deps_impl,
)
