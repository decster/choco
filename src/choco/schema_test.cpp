#include "gtest/gtest.h"
#include "schema.h"

namespace choco {

TEST(Schema, create) {
	unique_ptr<Schema> sc;
	ASSERT_TRUE(Schema::create("int32 id,int32 uv,int32 pv,int8 city null", sc));
	EXPECT_EQ(sc->get(1)->type, Type::Int32);
	EXPECT_EQ(sc->get(2)->type, Type::Int32);
	EXPECT_EQ(sc->get(3)->type, Type::Int32);
	EXPECT_EQ(sc->get(4)->type, Type::Int8);
}

}
