#include "gtest/gtest.h"
#include "choco/column.h"

namespace choco {

TEST(Column, int8notnull) {
    ColumnSchema cs("colint8", 1, Type::Int8, false);
    RefPtr<Column> c(new Column(cs, Type::Int8, 1));
    unique_ptr<ColumnWriter> writer;
    ASSERT_TRUE(c->write(writer));
    vector<uint8_t> values(10000, 0);
    for (size_t i=0;i<values.size();i++) {
        values[i] = (uint8_t)HashCode(i);
        EXPECT_TRUE(writer->insert(0, &values[i]));
    }
    RefPtr<Column> newc;
    ASSERT_TRUE(writer->finalize(2, newc));
    unique_ptr<ColumnReader> readc;
    ASSERT_TRUE(newc->read(2, readc));
    for (uint32_t i=0;i<values.size();i++) {
        uint8_t value = 0;
        EXPECT_TRUE(readc->get(i, &value));
        EXPECT_EQ(value, values[i]);
    }
}

}
