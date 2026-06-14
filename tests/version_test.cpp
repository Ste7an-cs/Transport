#include "transport/version.hpp"

#include <gtest/gtest.h>

TEST(Version, ReturnsNonEmpty) {
  EXPECT_FALSE(transport::LibraryVersion().empty());
}

TEST(Version, MatchesSemanticConstant) {
  EXPECT_EQ(transport::LibraryVersion(), "0.1.0");
  EXPECT_EQ(transport::LibraryVersion(), transport::kVersion);
  EXPECT_EQ(transport::kVersionMajor, 0);
  EXPECT_EQ(transport::kVersionMinor, 1);
  EXPECT_EQ(transport::kVersionPatch, 0);
}
