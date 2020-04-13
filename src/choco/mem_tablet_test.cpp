#include "gtest/gtest.h"
#include "mem_tablet.h"

namespace choco {

TEST(MemTablet, create) {
	unique_ptr<Schema> sc;
	ASSERT_TRUE(Schema::create("int32 id,int32 uv,int32 pv,int8 city null", sc));
	shared_ptr<MemTablet> tablet;
	ASSERT_TRUE(MemTablet::create("", sc, tablet));

}

}
