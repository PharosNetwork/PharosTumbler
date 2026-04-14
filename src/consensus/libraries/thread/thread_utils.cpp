// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include "consensus/libraries/thread/thread_utils.h"

#include <string.h>
#include <pthread.h>

namespace consensus_spec {
namespace thread_utils {

void SetCurrentThreadName(const char* name) {
#ifdef __USE_GNU
    if (name == nullptr) {
        return;
    }

    char n[16] = {0};
    auto len = strlen(name);
    if (len > 15) {
        len = 15;
    }
    memcpy(n, name, len);
    n[len] = 0;

    pthread_setname_np(pthread_self(), n);
#endif
}

}  // namespace thread_utils
}  // namespace consensus_spec
