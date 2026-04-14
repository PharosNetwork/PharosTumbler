load("@cppssz//bazel:codegen.bzl", "cc_ssz_library")
package(default_visibility = ["//visibility:public"])

cc_ssz_library(
    name = "consensus_cc_ssz",
    src = "consensus.ssz",
    include_dir = "src/consensus/schema",
)
