#include "gtest/gtest.h"
#include "mem_tablet.h"

namespace choco {

TEST(MemTablet, write) {
    const int num_insert = 1000000;
    const int num_update = 10000;
    const int update_time = 5;
	unique_ptr<Schema> sc;
	ASSERT_TRUE(Schema::create("int32 id,int32 uv,int32 pv,int8 city null", sc));
	shared_ptr<MemTablet> tablet;
	ASSERT_TRUE(MemTablet::create("", sc, tablet));
	uint64_t cur_version = 0;

	unique_ptr<WriteTx> wtx;
	EXPECT_TRUE(tablet->create_writetx(wtx));

    srand(1);
    PartialRowWriter writer(wtx->schema());
    PartialRowBatch* batch = wtx->new_batch();
    size_t nrow = 0;
    for (int i=0;i<num_insert;i++) {
        nrow++;
        writer.start_row();
        int id = i;
        int uv = rand() % 10000;
        int pv = rand() % 10000;
        int8_t city = rand() % 100;
        EXPECT_TRUE(writer.set("id", &id));
        EXPECT_TRUE(writer.set("uv", &uv));
        EXPECT_TRUE(writer.set("pv", &pv));
        EXPECT_TRUE(writer.set("city", city%2==0?nullptr:&city));
        if (!writer.write_row_to_batch(*batch)) {
            batch = wtx->new_batch();
            EXPECT_TRUE(writer.write_row_to_batch(*batch));
        }
    }
    EXPECT_TRUE(tablet->prepare_writetx(wtx));
	EXPECT_TRUE(tablet->commit(wtx, ++cur_version));

	// update
	for (int i=0;i<update_time;i++) {
	    wtx.reset();
	    EXPECT_TRUE(tablet->create_writetx(wtx));
	    batch = wtx->new_batch();
	    size_t nrow = 0;
	    for (int i=0;i<num_update;i++) {
	        nrow++;
	        writer.start_row();
	        int id = rand() % num_insert;
	        int uv = rand() % 10000;
	        int pv = rand() % 10000;
	        int8_t city = rand() % 100;
	        EXPECT_TRUE(writer.set("id", &id));
	        EXPECT_TRUE(writer.set("pv", &pv));
	        EXPECT_TRUE(writer.set("city", city%2==0?nullptr:&city));
	        if (!writer.write_row_to_batch(*batch)) {
	            batch = wtx->new_batch();
	            EXPECT_TRUE(writer.write_row_to_batch(*batch));
	        }
	    }
	    EXPECT_TRUE(tablet->prepare_writetx(wtx));
	    EXPECT_TRUE(tablet->commit(wtx, ++cur_version));
	}

}

}
