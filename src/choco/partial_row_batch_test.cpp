#include "gtest/gtest.h"
#include "partial_row_batch.h"
#include "string_pool.h"

namespace choco {

TEST(PartialRowbatch, write) {
    unique_ptr<Schema> sc;
    ASSERT_TRUE(Schema::create("int32 id,int32 uv,int32 pv,int8 city null,string ss null", sc));
    PartialRowBatch rb(*sc, 100000, 1000);
    PartialRowWriter writer(*sc);
    srand(1);
    const int N = 1000;
    size_t nrow = 0;
    for (int i=0;i<N;i++) {
        nrow++;
        writer.start_row();
        int id = i;
        int uv = rand() % 10000;
        int pv = rand() % 10000;
        int8_t city = rand() % 100;
        string sss = Format("ss%010d", rand()%10000000);
        Slice ss(sss);
        EXPECT_TRUE(writer.set("id", &id));
        if (i%3 == 0) {
            EXPECT_TRUE(writer.set("uv", &uv));
            EXPECT_TRUE(writer.set("pv", &pv));
            EXPECT_TRUE(writer.set("city", city%2==0?nullptr:&city));
            EXPECT_TRUE(writer.set("ss", &ss));
        }
        EXPECT_TRUE(writer.write_row_to_batch(rb));
    }
    EXPECT_EQ(rb.row_size(), nrow);

    PartialRowReader reader(rb);
    srand(1);
    for (size_t i=0;i<reader.size();i++) {
        EXPECT_TRUE(reader.read(i));
        if (i%3==0) {
            EXPECT_EQ(reader.cell_size(), 5);
        } else {
            EXPECT_EQ(reader.cell_size(), 1);
        }
        int id = i;
        int uv = rand() % 10000;
        int pv = rand() % 10000;
        int8_t city = rand() % 100;
        string sss = Format("ss%010d", rand()%10000000);

        const ColumnSchema* cs=nullptr;
        const void* data=nullptr;

        EXPECT_TRUE(reader.get_cell(0, cs, data));
        EXPECT_EQ(cs->cid, 1);
        EXPECT_EQ(*(int32_t*)data, id);

        if (i%3 == 0) {
            EXPECT_TRUE(reader.get_cell(1, cs, data));
            EXPECT_EQ(cs->cid, 2);
            EXPECT_EQ(*(int32_t*)data, uv);

            EXPECT_TRUE(reader.get_cell(2, cs, data));
            EXPECT_EQ(cs->cid, 3);
            EXPECT_EQ(*(int32_t*)data, pv);

            EXPECT_TRUE(reader.get_cell(3, cs, data));
            EXPECT_EQ(cs->cid, 4);
            if (city % 2 == 0) {
                EXPECT_EQ(data, nullptr);
            } else {
                EXPECT_EQ(*(int8_t*)data, city);
            }

            EXPECT_TRUE(reader.get_cell(4, cs, data));
            EXPECT_EQ(cs->cid, 5);
            SString* sstr = (SString*)data;
            EXPECT_EQ(sstr->to_string(), sss);
        }
    }
}

}
