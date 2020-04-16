#include "gtest/gtest.h"
#include "mem_tablet.h"
#include "mem_tablet_scan.h"

namespace choco {

struct TData {
    int32_t id;
    int32_t uv;
    int32_t pv;
    int8_t city;
};

TEST(MemTablet, writescan) {
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

	vector<TData> alldata(num_insert);

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
        alldata[i].id = id;
        alldata[i].uv = uv;
        alldata[i].pv = pv;
        alldata[i].city = city;
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
    wtx.reset();

	// update
	for (int i=0;i<update_time;i++) {
	    EXPECT_TRUE(tablet->create_writetx(wtx));
	    batch = wtx->new_batch();
	    size_t nrow = 0;
	    for (int j=0;j<num_update;j++) {
	        nrow++;
	        writer.start_row();
	        int id = rand() % num_insert;
	        int uv = rand() % 10000;
	        int pv = rand() % 10000;
	        int8_t city = rand() % 100;
	        alldata[id].uv = uv;
	        alldata[id].pv = pv;
	        alldata[id].city = city;
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
	    wtx.reset();
	}

	unique_ptr<ScanSpec> scanspec;
	ASSERT_TRUE(ScanSpec::create(cur_version, "pv", scanspec));
	unique_ptr<MemTabletScan> scan;
	ASSERT_TRUE(tablet->scan(scanspec, scan));
	const RowBlock* rblock = nullptr;
    size_t curidx = 0;
	while (true) {
	    EXPECT_TRUE(scan->next_block(rblock));
	    if (!rblock) {
	        break;
	    }
	    size_t nrows = rblock->num_rows();
	    const ColumnBlock& cb = rblock->get_column(0);
	    for (size_t i=0;i<nrows;i++) {
	        int32_t value = ((const int32_t*)cb.data())[i];
	        EXPECT_EQ(value, alldata[curidx].pv);
	        curidx++;
	    }
	}
	EXPECT_EQ(curidx, num_insert);
	scan.reset();
}

}
