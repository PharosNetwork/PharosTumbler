"""Module extension for git-backed deps that must run before most third_party rules."""

load("//third_party/cpp_thirdparty:cpp_thirdparty.bzl", "cpp_thirdparty_deps")

def _deps_impl(_ctx):
    cpp_thirdparty_deps()

deps = module_extension(
    implementation = _deps_impl,
)
