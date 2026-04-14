// (C) 2016-2026 Ant Digital Technologies Co.,Ltd.
// SPDX-License-Identifier: Apache-2.0

// These tests fail until cppssz fixes ByteList::{operator=(ByteList&&), ByteList(ByteList&&)}
// to refresh view_ from holder_ after swapping holders (e.g. reassign view from
// std::get<std::string>(holder_).data() / size).
//
// ByteList move semantics (cppssz): operator=(ByteList&&) and ByteList(ByteList&&) both do
//   swap(holder_, other.holder_); swap(view_, other.view_);
//
// other.view_ is not a "temporary" in the C++ sense: it is a string_view that was valid while
// the std::string lived in other's variant storage. After swap(holder_), that string object
// (and its SSO buffer / heap pointer) now lives in the other ByteList's variant — the character
// storage moved. Swapping the string_view bits alone does not re-point view_ at the string's new
// location, so no-arg Acquire() can disagree with Acquire<std::string>().
#include <gtest/gtest.h>

#include <limits>
#include <string>

#include <cppssz/ssz_base.h>

namespace consensus_spec {

namespace {

bool acquire_view_matches_string_holder(const ssz::ByteList<0, 32>& bl) {
    const std::string_view v = bl.Acquire();
    const std::string& s = bl.Acquire<std::string>();
    return v.size() == s.size() && v.data() == s.data();
}

}  // namespace

TEST(ByteListCppssz, StringCtor_NoArgAcquireMatchesHolder) {
    const std::string logical = "test_hash";
    ssz::ByteList<0, 32> bl{std::string(logical)};
    EXPECT_TRUE(acquire_view_matches_string_holder(bl));
    EXPECT_EQ(std::string(bl.Acquire()), logical);
}

TEST(ByteListCppssz, ImplicitAssignFromString_NoArgAcquireMatchesHolder) {
    const std::string logical = "test_hash";
    ssz::ByteList<0, 32> bl;
    bl = logical;  // ByteList(std::string) temporary + operator=(ByteList&&)
    EXPECT_TRUE(acquire_view_matches_string_holder(bl))
        << "no-arg Acquire() should view the same bytes as std::string in holder_; "
           "if this fails, fix ByteList move assign (refresh view_ after swap(holder_))";
    EXPECT_EQ(std::string(bl.Acquire()), logical);
}

TEST(ByteListCppssz, SetData_NoArgAcquireMatchesHolder) {
    const std::string logical = "test_hash";
    ssz::ByteList<0, 32> bl;
    bl.SetData(std::string(logical));  // *this = ByteList(std::move(s))
    EXPECT_TRUE(acquire_view_matches_string_holder(bl))
        << "SetData uses move-assign; same view_/holder_ requirement as implicit assign";
    EXPECT_EQ(std::string(bl.Acquire()), logical);
}

TEST(ByteListCppssz, MoveCtorFromXvalue_NoArgAcquireMatchesHolder) {
    const std::string logical = "test_hash";
    ssz::ByteList<0, 32> tmp{std::string(logical)};
    ssz::ByteList<0, 32> bl{std::move(tmp)};  // must use xvalue; prvalue assignment often elides
    EXPECT_TRUE(acquire_view_matches_string_holder(bl))
        << "ByteList(ByteList&&) uses the same swap pair; view_ must stay aligned after move";
    EXPECT_EQ(std::string(bl.Acquire()), logical);
}

}  // namespace consensus_spec
