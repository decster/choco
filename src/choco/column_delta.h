#ifndef CHOCO_COLUMN_DELTA_H_
#define CHOCO_COLUMN_DELTA_H_

#include "common.h"
#include "type.h"
#include "buffer.h"

namespace choco {

class ColumnDelta;


class DeltaIndex : public RefCounted {
public:
    static const uint32_t npos = 0xffffffffu;

    DeltaIndex() = default;

    size_t memory() const;

    uint32_t find_idx(uint32_t rid);

    Buffer& index() { return _data; }

    vector<uint32_t> _block_ends;
    Buffer _data;

//    struct alignas(64) BlockIdx {
//        uint16_t samples[32];
//    };
//    vector<BlockIdx> _block_idxs;
};


class ColumnDelta : public RefCounted {
public:
    ColumnDelta() = default;

    size_t memory() const;

    Buffer& nulls() { return _nulls; }

    Buffer& data() { return _data; }

    DeltaIndex* index() { return _index.get(); }

    uint32_t find_idx(uint32_t rid) { return _index->find_idx(rid); }

    Status alloc(size_t nblock, size_t size, size_t esize, BufferTag tag, bool has_null);

    Status create_for_compaction(RefPtr<ColumnDelta>& ret);

private:
    size_t _size = 0;
    BufferTag _tag = 0;
    RefPtr<DeltaIndex> _index;
    Buffer _nulls;
    Buffer _data;
};



} /* namespace choco */

#endif /* CHOCO_COLUMN_DELTA_H_ */
