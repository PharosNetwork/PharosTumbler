# PharosTumbler Consensus Library

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Language](https://img.shields.io/badge/Language-C++20-orange.svg)](https://isocpp.org/)

## Project Overview

PharosTumbler is a high-performance multi-leader BFT (Byzantine Fault Tolerant) consensus library designed for blockchain and distributed ledger systems. Built upon the research presented in [SOSP 2023: Flexible Advancement in Asynchronous BFT Consensus](https://dl.acm.org/doi/10.1145/3600006.3613164) (note: the current implementation operates under a partial synchrony assumption rather than the fully asynchronous model in the paper), it provides a production-ready multi-leader consensus protocol stack that achieves low latency and high throughput under Proof-of-Stake (PoS) settings.

PharosTumbler operates as a **consensus library** that integrates into a host blockchain platform — it is not a standalone runnable node, but a modular consensus engine that can be embedded into various blockchain architectures.

### Core Features

- **Multi-Leader Proposal** - Multiple validators propose blocks in parallel, eliminating single-leader bandwidth bottleneck and enabling horizontal throughput scaling
- **Pass Advancement Mechanism** - Validators without pending transactions can fast-skip through consensus rounds, avoiding empty-block overhead and reducing latency under light or uneven load
- **Optimized Binary Agreement** - Fast-path consensus when validators agree; randomized fallback only when needed, significantly reducing tail latency on WANs
- **Reliable Channel with WAL** - Built-in reliable message delivery with receipt/retransmission and Write-Ahead Log for crash recovery and state replay
- **PoS Native** - Voting weight is proportional to stake; all thresholds and quorum checks are stake-weighted (tolerates up to 1/3 Byzantine stake)

## Architecture

PharosTumbler adopts a layered modular architecture:

```
┌───────────────────────────────────────────────────────┐
│              Host Platform (Network / Identity)       │
├───────────────────────────────────────────────────────┤
│                   Tumbler Engine                      │
│  ┌─────────────────────────────────────────────────┐  │
│  │  Pipeline Coordination & Sequence Ordering      │  │
│  │  (Propose → Route → Aggregate → Pass → Commit)  │  │
│  ├─────────────────────────────────────────────────┤  │
│  │  MyBA: Multi-Value Consensus Instances          │  │
│  │  (Per-sequence voting, certificate generation)  │  │
│  ├─────────────────────────────────────────────────┤  │
│  │  Reliable Channel + WAL Persistence             │  │
│  │  (Reliable broadcast, retransmission, replay)   │  │
│  ├─────────────────────────────────────────────────┤  │
│  │  Crypto Helper                                  │  │
│  │  (Callbacks injected by host platform)          │  │
│  └─────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────┘
```

### Core Modules

| Module | Description |
|--------|-------------|
| **Tumbler Engine** | Top-level coordination layer: manages multi-leader proposals and skips, drives MyBA instances, orchestrates Pass advancement, and delivers final consensus results |
| **MyBA** | Per-proposal consensus instance: votes on whether a specific (seq, proposer) proposal should be committed or aborted, generates aggregated certificates when quorum is reached |
| **Reliable Channel** | Message delivery layer: reliable send/broadcast with receipt tracking, retransmission, and WAL-based persistence and replay |
| **Crypto Helper** | Abstraction layer for cryptographic primitives (digest, signature, aggregation); no built-in implementation — concrete callbacks are injected by the host platform |

## How It Works

The consensus flow for each sequence number follows this high-level path:

1. **Propose / Skip** - Multiple validators can propose their own candidate blocks in parallel; validators with nothing to propose declare a Skip
2. **MyBA Consensus** - Each proposal is bound to a dedicated MyBA instance, which collects votes from validators and works toward committing or aborting that proposal
3. **Pass** - When enough MyBA instances have been committed or skipped, the Pass phase begins, attempting to finalize the remaining undecided MyBA instances
4. **Consensus Complete** - All committed proposals are assembled into the final consensus result for this sequence and delivered to the host platform
5. **Pipeline** - While one sequence finalizes, subsequent sequences can begin proposal and voting in parallel

## Directory Structure

```
src/consensus/
├── common/          # Crypto abstractions, node identity, utility types
├── libraries/
│   ├── thread/      # Async worker (asio io_context + timers)
│   ├── wal/         # Write-Ahead Log for persistence and replay
│   ├── utils/       # Common utilities
│   └── log/         # Logging
├── protocol/
│   ├── myba.*                      # BA sub-protocol implementation
│   ├── mytumbler_engine_base.*     # MyTumbler coordination engine
│   ├── mytumbler_message_types.*   # Message types, digest, indexing
│   └── reliable_channel.*          # Reliable delivery + WAL integration
└── schema/
    └── consensus.ssz               # SSZ message schema definitions
```

## Build & Test

### Environment Requirements

- **Operating System**: Linux (CentOS 7+, Ubuntu 24.04+)
- **Compiler**: GCC 13+
- **Build Tool**: Bazel 8.1.0+

> **Note**: Due to missing `third_party` and other internal dependencies, the project cannot be directly compiled at this time. The build instructions below are provided for reference once dependencies are resolved.

### Build

```bash
# Build test binary
bazel build //test:test_consensus --jobs=8 --compilation_mode=dbg
```

### Run Tests

```bash
# Run all tests
bazel run //test:test_consensus -- --gtest_color=yes
```

Optional: Enable console log output:

```bash
bazel run //test:test_consensus -- --enable_print_console=true
```

Optional: Run specific test cases (gtest filter):

```bash
bazel run //test:test_consensus -- --gtest_filter=MySuiteName.MyCaseName
```

> `//test:test_consensus` is the gtest entry point in `test/test_helper/test_consensus.cpp`, which initializes logging and executes `RUN_ALL_TESTS()` by default.

## References

1. Shengyun Liu, Wenbo Xu, Chen Shan, Xiaofeng Yan, Tianjing Xu, Bo Wang, Lei Fan, Fuxi Deng, Ying Yan, Hui Zhang. *Flexible Advancement in Asynchronous BFT Consensus.* SOSP 2023. [ACM DL](https://dl.acm.org/doi/10.1145/3600006.3613164)

## License

This project is licensed under the [Apache 2.0](LICENSE) License.
