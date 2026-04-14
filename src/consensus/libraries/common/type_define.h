// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <string>
#include <utility>
#include <vector>

namespace consensus_spec {

using View = uint64_t;
using Seq = uint64_t;
using NodeId = std::string;
// TODO(v13): change to uint32_t, requires compatibility upgrade
using Round = uint8_t;
using Timestamp = uint64_t;
using bytes = std::vector<uint8_t>;
using Digest = std::string;
using Signature = std::string;
using PrivateKey = std::string;
using PublicKey = std::string;

/* BitMap data structure listed below:
0               1               2               3   (bytes)
-------------------------------------------------
|             MycryptoId        |   Remainder   |
-------------------------------------------------
|                                               |
|                   Map Data                    |
|                                               |
-------------------------------------------------
*/

using BitMap = std::string;

enum AggregatedMainVoteType : uint8_t {
    Prom = 0,
    Aux
};
}  // namespace consensus_spec
