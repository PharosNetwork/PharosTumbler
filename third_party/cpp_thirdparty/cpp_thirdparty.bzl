load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def cpp_thirdparty_deps():
    git_repository(
        name = "PLACEHOLDER_cpp_thirdparty",
        tag  = "PLACEHOLDER",
        # commit = "",
        # TODO: replace with public repository URL
        remote = "PLACEHOLDER",
    )
