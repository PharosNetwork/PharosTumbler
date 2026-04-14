# 需要从 genrule 生成的头文件（.in → .h + 纯生成的）
GENERATED_HEADERS = [
    # 纯生成的（源码中没有）
    "configuration.h",
    "symbol_prefix.h",
    # 从 .in 生成的（源码中只有 .in 文件）
    "asn1.h",
    "asn1t.h",
    "bio.h",
    "cmp.h",
    "cms.h",
    "conf.h",
    "crmf.h",
    "crypto.h",
    "ct.h",
    "err.h",
    "ess.h",
    "fipskey.h",
    "lhash.h",
    "ocsp.h",
    "opensslv.h",
    "pkcs12.h",
    "pkcs7.h",
    "safestack.h",
    "srp.h",
    "ssl.h",
    "ui.h",
    "x509.h",
    "x509_vfy.h",
    "x509v3.h",
]

genrule(
    name = "openssl-build",
    srcs = glob(["**/*"], exclude=["bazel-*"]),
    outs = [
        "libcrypto.a",
        "libssl.a",
    ] + ["include/openssl/" + h for h in GENERATED_HEADERS],
    cmd = """
        OPENSSL_ROOT=$$(dirname $(location config))
        BUILD_OUT=$$(dirname $(location libcrypto.a))
        
        pushd $$OPENSSL_ROOT
            ./config
            make build_generated
            make -j8 libcrypto.a libssl.a
        popd
        
        # 复制库文件
        cp $$OPENSSL_ROOT/libssl.a $(location libssl.a)
        cp $$OPENSSL_ROOT/libcrypto.a $(location libcrypto.a)
        
        # 批量复制生成的头文件
        mkdir -p $$BUILD_OUT/include/openssl
        for h in configuration.h symbol_prefix.h asn1.h asn1t.h bio.h cmp.h cms.h conf.h crmf.h crypto.h ct.h err.h ess.h fipskey.h lhash.h ocsp.h opensslv.h pkcs12.h pkcs7.h safestack.h srp.h ssl.h ui.h x509.h x509_vfy.h x509v3.h; do
            if [ -f $$OPENSSL_ROOT/include/openssl/$$h ]; then
                cp $$OPENSSL_ROOT/include/openssl/$$h $$BUILD_OUT/include/openssl/$$h
            fi
        done
    """,
)

cc_library(
    name = "crypto",
    hdrs = glob([
        "include/**/*.h",
        "include/internal/*.h",
        "crypto/**/*.h",
        "providers/**/*.h",
    ], exclude = [
        "**/*.h.in",
    ]) + ["include/openssl/" + h for h in GENERATED_HEADERS],
    srcs = [":openssl-build"],  # ← 改这行，从 ["libcrypto.a"] 改成 [":openssl-build"]
    includes = ["include", "crypto/include"],
    linkopts = ["-lpthread", "-ldl"],
    visibility = ["//visibility:public"],
)
cc_library(
    name = "ssl",
    deps = [":crypto"],
    hdrs = glob([
        "include/openssl/*.h",
    ], exclude = [
        "**/*.h.in",
    ]) + ["include/openssl/" + h for h in GENERATED_HEADERS],
    srcs = [":openssl-build"],  # ← 改这行，从 ["libssl.a"] 改成 [":openssl-build"]
    includes = ["include", "crypto/include"],
    visibility = ["//visibility:public"],
)