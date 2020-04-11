#include "gtest/gtest.h"
#include "column_delta.h"
#include "column.h"

namespace choco {

TEST(ColumnDelta, Index) {
    const int BaseSize = 256001;
    const int NumUpdate = 10000;
    srand(1);
    RefPtr<ColumnDelta> delta = RefPtr<ColumnDelta>::create();
    std::map<uint32_t, uint32_t> updates;
    for (int i=0;i<NumUpdate;i++) {
        uint32_t idx = rand() % BaseSize;
        updates[idx] = rand();
    }
    size_t nblock = NBlock(BaseSize, Column::BLOCK_SIZE);
    DLOG(INFO) << Format("nblock=%zu", nblock);
    ASSERT_TRUE(delta->alloc(nblock, updates.size(), sizeof(uint32_t),
            BufferTag::delta(1), false));
    DeltaIndex* index = delta->index();
    vector<uint32_t>& block_ends = index->_block_ends;
    Buffer& idxdata = index->_data;
    Buffer& data = delta->data();
    Buffer& nulls = delta->nulls();
    uint32_t cidx = 0;
    uint32_t curbid = 0;
    for (auto& e : updates) {
        uint32_t rid = e.first;
        uint32_t bid = rid >> 16;
        while (curbid < bid) {
            block_ends[curbid] = cidx;
            curbid++;
        }
        idxdata.as<uint16_t>()[cidx] = rid & 0xffff;
        data.as<uint32_t>()[cidx] = e.second;
        cidx++;
    }
    while (curbid < nblock) {
        block_ends[curbid] = cidx;
        curbid++;
    }
    for (int i=0;i<BaseSize;i++) {
        uint32_t idx = delta->find_idx(i);
        auto itr = updates.find(i);
        if (itr == updates.end()) {
            EXPECT_TRUE(idx == DeltaIndex::npos);
        } else {
            uint32_t v = delta->data().as<uint32_t>()[idx];
            EXPECT_EQ(v, itr->second);
        }
    }
}

}
