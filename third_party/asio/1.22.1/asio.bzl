load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def asio_1_22_1_deps():
    http_archive(
        name = "asio_1_22_1",
        build_file = "//third_party/asio/1.22.1:asio.BUILD",
        sha256 = "PLACEHOLDER",
        strip_prefix = "asio-1.22.1",
        urls = [
            # TODO: replace with public mirror URL for asio-1.22.1.tar.gz
        ],
        patches = [
            "//third_party/asio/1.22.1:asio.patch",
        ],
        patch_args = [
            "-p0",
        ],

    )


