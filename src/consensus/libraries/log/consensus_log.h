// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

#include <string_view>

#include <cobre/libraries/log/logging.h>

const std::string ConsensusLog = "consensus";

// ###################### Compile-time log placeholder check ##################
// Compile-time placeholder count (supports {} format, handles escaped {{ and }})
constexpr size_t count_placeholders(std::string_view str) noexcept {
    size_t count = 0;
    for (size_t i = 0; i < str.size(); ++i) {
        if (str[i] == '{') {
            if (i + 1 < str.size()) {
                if (str[i + 1] == '{') {
                    // escaped {{
                    ++i;  // skip the second {
                } else if (str[i + 1] == '}') {
                    // placeholder {}
                    ++count;
                    ++i;  // skip }
                }
            }
        } else if (str[i] == '}') {
            if (i + 1 < str.size() && str[i + 1] == '}') {
                // escaped }}
                ++i;  // skip the second }
            }
        }
    }
    return count;
}

// Argument count macro
#define COUNT_ARGS(...) std::tuple_size<decltype(std::make_tuple(__VA_ARGS__))>::value
#define ASSERT_CHECK_ARGS(fmt_str, args...)                        \
    static_assert(count_placeholders(fmt_str) == COUNT_ARGS(args), \
                  "[COMPILE ERROR] Placeholder count mismatch. (fmt: " fmt_str ")")
// ###################### Compile-time log placeholder check ##################

#define _CS_LOG_TRACE_(log_name, fmt_str, args...)                             \
    do {                                                                       \
        ASSERT_CHECK_ARGS(fmt_str, ##args);                                    \
        auto static logger = spdlog::get(log_name);                            \
        if (logger && (logger->level() <= spdlog::level::level_enum::trace)) { \
            SPDLOG_LOGGER_TRACE(logger, fmt_str, ##args);                      \
        }                                                                      \
    } while (false)

#define _CS_LOG_DEBUG_(log_name, fmt_str, args...)                             \
    do {                                                                       \
        ASSERT_CHECK_ARGS(fmt_str, ##args);                                    \
        auto static logger = spdlog::get(log_name);                            \
        if (logger && (logger->level() <= spdlog::level::level_enum::debug)) { \
            SPDLOG_LOGGER_DEBUG(logger, fmt_str, ##args);                      \
        }                                                                      \
    } while (false)

#define _CS_LOG_INFO_(log_name, fmt_str, args...)                             \
    do {                                                                      \
        ASSERT_CHECK_ARGS(fmt_str, ##args);                                   \
        auto static logger = spdlog::get(log_name);                           \
        if (logger && (logger->level() <= spdlog::level::level_enum::info)) { \
            SPDLOG_LOGGER_INFO(logger, fmt_str, ##args);                      \
        }                                                                     \
    } while (false)

#define _CS_LOG_WARN_(log_name, fmt_str, args...)                             \
    do {                                                                      \
        ASSERT_CHECK_ARGS(fmt_str, ##args);                                   \
        auto static logger = spdlog::get(log_name);                           \
        if (logger && (logger->level() <= spdlog::level::level_enum::warn)) { \
            SPDLOG_LOGGER_WARN(logger, fmt_str, ##args);                      \
        }                                                                     \
    } while (false)

#define _CS_LOG_ERROR_(log_name, fmt_str, args...)                           \
    do {                                                                     \
        ASSERT_CHECK_ARGS(fmt_str, ##args);                                  \
        auto static logger = spdlog::get(log_name);                          \
        if (logger && (logger->level() <= spdlog::level::level_enum::err)) { \
            SPDLOG_LOGGER_ERROR(logger, fmt_str, ##args);                    \
        }                                                                    \
    } while (false)

#define _CS_LOG_FATAL_(log_name, fmt_str, args...)                                \
    do {                                                                          \
        ASSERT_CHECK_ARGS(fmt_str, ##args);                                       \
        auto static logger = spdlog::get(log_name);                               \
        if (logger && (logger->level() <= spdlog::level::level_enum::critical)) { \
            SPDLOG_LOGGER_CRITICAL(logger, fmt_str, ##args);                      \
        }                                                                         \
    } while (false)

#define CS_LOG_TRACE(fmt_str, args...) _CS_LOG_TRACE_(ConsensusLog, fmt_str, ##args)
#define CS_LOG_DEBUG(fmt_str, args...) _CS_LOG_DEBUG_(ConsensusLog, fmt_str, ##args)
#define CS_LOG_INFO(fmt_str, args...)  _CS_LOG_INFO_(ConsensusLog, fmt_str, ##args)
#define CS_LOG_WARN(fmt_str, args...)  _CS_LOG_WARN_(ConsensusLog, fmt_str, ##args)
#define CS_LOG_ERROR(fmt_str, args...) _CS_LOG_ERROR_(ConsensusLog, fmt_str, ##args)
#define CS_LOG_FATAL(fmt_str, args...) _CS_LOG_FATAL_(ConsensusLog, fmt_str, ##args)