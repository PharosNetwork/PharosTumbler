load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
def cobre_deps():
    git_repository(
        name = "cobre",
        # TODO: replace with public repository URL for cobre
        remote = "PLACEHOLDER",
        # branch = "origin/dev",
        #commit = "",
        tag = "PLACEHOLDER",
        verbose = True,
    )
# def cobre_deps():
    # native.local_repository(
        # name = "cobre",
        # path = "/home/admin/workspace/cobre"
    # )
