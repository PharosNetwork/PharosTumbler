
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
def storage_service_deps():
    git_repository(
        name = "storageservice",
        tag = "PLACEHOLDER",
        #commit = "",
        # TODO: replace with public repository URL for storageservice
        remote = "PLACEHOLDER",
        verbose = True,
    )
