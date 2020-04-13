#include "gtest/gtest.h"
#include "slice.h"

namespace choco {

TEST(Slice, trim) {
	EXPECT_TRUE(Slice("  asdb  ").trim() == Slice("asdb"));
	EXPECT_TRUE(Slice("").trim() == Slice(""));
	EXPECT_TRUE(Slice("asdb  ").trim() == Slice("asdb"));
	EXPECT_TRUE(Slice("  asdb").trim() == Slice("asdb"));
	EXPECT_TRUE(Slice("  \tasdb\t  ").trim() == Slice("asdb"));
	EXPECT_TRUE(Slice("  \tasdb adf  ").trim() == Slice("asdb adf"));
}

TEST(Slice, join) {
	std::vector<Slice> strs({"aaa", "bbb", "ccc"});
	EXPECT_EQ(Slice::join(strs, " "), string("aaa bbb ccc"));
}

TEST(Slice, split) {
	EXPECT_TRUE(Slice("aaa bbb ccc").split(' ', false) == std::vector<Slice>({"aaa", "bbb", "ccc"}));
	EXPECT_TRUE(Slice("aaa  ccc").split(' ', false) == std::vector<Slice>({"aaa", "", "ccc"}));
	EXPECT_TRUE(Slice("").split(' ', false) == std::vector<Slice>({""}));
	EXPECT_TRUE(Slice("aaa  ccc").split(' ', true) == std::vector<Slice>({"aaa", "ccc"}));
	EXPECT_TRUE(Slice("").split(' ', true) == std::vector<Slice>());
}

}
