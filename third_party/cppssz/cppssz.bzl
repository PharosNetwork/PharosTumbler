"""cppssz: pinned here so consensus does not follow @cobre's default tag."""

load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

def cppssz_deps():
    git_repository(
        name = "cppssz",
        tag = "PLACEHOLDER",
        # TODO: replace with public repository URL for cppssz
        remote = "PLACEHOLDER",
        verbose = True,
    )
