#include "gtest/gtest.h"
#include "choco/column.h"

namespace choco {

template <Type TypeT>
struct ColumnTest {
    static const size_t InsertCount = 5000000;
    static const size_t UpdateTime = 70; //70;
    static const size_t UpdateCount = 10000;
    typedef typename TypeTrait<TypeT>::cpp_type CppType;

    static bool is_null(CppType v) {
        return ((int64_t)v) % 10 == 0;
    }

    static void test_not_null() {
        ColumnSchema cs(TypeTrait<TypeT>::name(), 1, TypeT, false);
        RefPtr<Column> c(new Column(cs, TypeT, 1));
        unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->write(writer));
        vector<CppType> values(InsertCount, 0);
        for (size_t i=0;i<values.size();i++) {
            values[i] = (CppType)HashCode(i);
            EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]));
        }
        RefPtr<Column> newc;
        ASSERT_TRUE(writer->finalize(2, newc));
        EXPECT_TRUE(c != newc);
        unique_ptr<ColumnReader> readc;
        ASSERT_TRUE(newc->read(2, readc));
        for (uint32_t i=0;i<values.size();i++) {
            CppType value = 0;
            EXPECT_TRUE(readc->get(i, &value));
            EXPECT_EQ(value, values[i]);
        }
    }

    static void test_nullable() {
        ColumnSchema cs(TypeTrait<TypeT>::name(), 1, TypeT, true);
        RefPtr<Column> c(new Column(cs, TypeT, 1));
        unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->write(writer));
        vector<CppType> values(InsertCount, 0);
        for (size_t i=0;i<values.size();i++) {
            values[i] = (CppType)HashCode(i);
            if (is_null(values[i])) {
                // set to null
                EXPECT_TRUE(writer->insert((uint32_t)i, NULL));
            } else {
                EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]));
            }
        }
        RefPtr<Column> newc;
        ASSERT_TRUE(writer->finalize(2, newc));
        EXPECT_TRUE(c != newc);
        unique_ptr<ColumnReader> readc;
        ASSERT_TRUE(newc->read(2, readc));
        for (uint32_t i=0;i<values.size();i++) {
            CppType value = 0;
            if (is_null(values[i])) {
                EXPECT_FALSE(readc->get(i, &value));
            } else {
                EXPECT_TRUE(readc->get(i, &value));
                EXPECT_EQ(value, values[i]);
            }
        }
    }

    static void test_not_null_update() {
        srand(1);
        uint64_t version = 1;
        // insert
        ColumnSchema cs(TypeTrait<TypeT>::name(), version, TypeT, false);
        RefPtr<Column> c(new Column(cs, TypeT, 1));
        unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->write(writer));
        vector<CppType> values(InsertCount, 0);
        for (size_t i=0;i<values.size();i++) {
            values[i] = (CppType)HashCode(i);
            EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]));
        }
        ASSERT_TRUE(writer->finalize(++version, c));
        writer.reset();
        RefPtr<Column> oldc = c;
        for (size_t u=0;u<UpdateTime;u++) {
            ASSERT_TRUE(c->write(writer));
            vector<uint32_t> update_idxs;
            for (size_t i=0;i<UpdateCount;i++) {
                uint32_t idx = rand() % values.size();
                //CppType oldv = values[idx];
                values[idx] = (CppType)rand();
                //DLOG(INFO) << Format("update values[%zu] from %d to %d", idx, (int)oldv, (int)values[idx]);
                EXPECT_TRUE(writer->update(idx, &values[idx]));
                update_idxs.push_back(idx);
            }
            ASSERT_TRUE(writer->finalize(++version, c));
            //DLOG(INFO) << Format("update %zu writer: %s", u, writer->to_string().c_str());
            writer.reset();
            unique_ptr<ColumnReader> readc;
            ASSERT_TRUE(c->read(version, readc));
            //DLOG(INFO) << Format("read %zu reader: %s", u, readc->to_string().c_str());
            for (uint32_t i : update_idxs) {
                CppType value = 0;
                EXPECT_TRUE(readc->get(i, &value));
                EXPECT_EQ(value, values[i]) << Format("values[%u]", i);
            }
        }
        if (UpdateTime > 64) {
            ASSERT_TRUE(oldc != c);
        }
    }

    static void test_nullable_update() {
        srand(1);
        uint64_t version = 1;
        // insert
        ColumnSchema cs(TypeTrait<TypeT>::name(), version, TypeT, true);
        RefPtr<Column> c(new Column(cs, TypeT, 1));
        unique_ptr<ColumnWriter> writer;
        ASSERT_TRUE(c->write(writer));
        vector<CppType> values(InsertCount, 0);
        for (size_t i=0;i<values.size();i++) {
            values[i] = (CppType)HashCode(i);
            if (is_null(values[i])) {
                // set to null
                EXPECT_TRUE(writer->insert((uint32_t)i, NULL));
            } else {
                EXPECT_TRUE(writer->insert((uint32_t)i, &values[i]));
            }
        }
        ASSERT_TRUE(writer->finalize(++version, c));
        writer.reset();
        RefPtr<Column> oldc = c;
        for (size_t u=0;u<UpdateTime;u++) {
            ASSERT_TRUE(c->write(writer));
            vector<uint32_t> update_idxs;
            for (size_t i=0;i<UpdateCount;i++) {
                uint32_t idx = rand() % values.size();
                values[idx] = (CppType)rand();
                if (is_null(values[idx])) {
                    EXPECT_TRUE(writer->update(idx, NULL));
                } else {
                    EXPECT_TRUE(writer->update(idx, &values[idx]));
                }
                update_idxs.push_back(idx);
            }
            ASSERT_TRUE(writer->finalize(++version, c));
            //DLOG(INFO) << Format("update %zu writer: %s", u, writer->to_string().c_str());
            writer.reset();
            unique_ptr<ColumnReader> readc;
            ASSERT_TRUE(c->read(version, readc));
            //DLOG(INFO) << Format("read %zu reader: %s", u, readc->to_string().c_str());
            for (uint32_t i : update_idxs) {
                CppType value = 0;
                if (is_null(values[i])) {
                    EXPECT_FALSE(readc->get(i, &value));
                } else {
                    EXPECT_TRUE(readc->get(i, &value));
                    EXPECT_EQ(value, values[i]) << Format("values[%u]", i);
                }
            }
        }
        if (UpdateTime > 64) {
            ASSERT_TRUE(oldc != c);
        }
    }

    static void test() {
        test_not_null();
        test_nullable();
    }

    static void test_update() {
        test_not_null_update();
        test_nullable_update();
    }
};


TEST(Column, insert) {
    ColumnTest<Int8>::test();
    ColumnTest<Int16>::test();
    ColumnTest<Int32>::test();
    ColumnTest<Int64>::test();
    ColumnTest<Int128>::test();
    ColumnTest<Float32>::test();
    ColumnTest<Float64>::test();
}

TEST(Column, update) {
    ColumnTest<Int8>::test_update();
    ColumnTest<Int16>::test_update();
    ColumnTest<Int32>::test_update();
    ColumnTest<Int64>::test_update();
    ColumnTest<Int128>::test_update();
    ColumnTest<Float32>::test_update();
    ColumnTest<Float64>::test_update();
}

}
