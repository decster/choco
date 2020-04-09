#include "gtest/gtest.h"
#include "string_pool.h"

namespace choco {

TEST(SString, SString) {
    MString& empty = (MString&)SString::Empty;
    unique_ptr<MString> tt(MString::create("abcdefg hijklmn"));
    unique_ptr<MString> tt2(MString::create("abcdefg hijklmn"));
    unique_ptr<MString> tt3(MString::create("abcdefg"));
    EXPECT_EQ(*tt, *tt2);
    EXPECT_LT(*tt3, *tt);
    EXPECT_LT(empty, *tt);
}


TEST(StringPool, basic) {
    RefPtr<StringPool> pool;
    ASSERT_TRUE(StringPool::create(pool));
    RefPtr<StringPool> old_pool = pool;
    uint32_t foo, bar;
    EXPECT_TRUE(StringPool::add(pool, "foo", foo));
    EXPECT_TRUE(StringPool::add(pool, "bar", bar));
    uint32_t idnull, idempty;
    EXPECT_TRUE(StringPool::add(pool, nullptr, idnull));
    EXPECT_TRUE(StringPool::add(pool, Slice(), idempty));
    EXPECT_EQ(idnull, 0);
    EXPECT_EQ(idempty, 1);
    EXPECT_EQ(pool->get(0), nullptr);
    EXPECT_EQ(pool->get(1)->len, 0);
    const SString* sfoo = pool->get(foo);
    EXPECT_TRUE(sfoo);
    EXPECT_EQ(sfoo->to_string(), "foo");
    const SString* sbar = pool->get(bar);
    EXPECT_TRUE(sbar);
    EXPECT_EQ(sbar->to_string(), "bar");
}

TEST(StringPool, expand) {
    RefPtr<StringPool> pool;
    ASSERT_TRUE(StringPool::create(pool));
    RefPtr<StringPool> old_pool = pool;
    const size_t N = 1000000;
    vector<uint32_t> ids(N,0);
    for (size_t i=0;i<N;i++) {
        EXPECT_TRUE(StringPool::add(pool, Format("%010zu", i), ids[i]));
    }
    EXPECT_TRUE(old_pool != pool);
    for (size_t i=0;i<N;i++) {
        auto s = pool->get(ids[i]);
        EXPECT_EQ(s->to_string(), Format("%010zu", i));
    }
}

}
