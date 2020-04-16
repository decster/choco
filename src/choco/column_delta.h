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

    void block_range(uint32_t bid, uint32_t& start, uint32_t& end) const {
        if (bid < _block_ends.size()) {
            start = bid > 0 ? _block_ends[bid-1] : 0;
            end = _block_ends[bid];
        }
        start = 0;
        end = 0;
    }

    bool contains_block(uint32_t bid) const {
        if (bid < _block_ends.size()) {
            return (bid > 0 ? _block_ends[bid-1] : 0) < _block_ends[bid];
        }
        return false;
    }

    Buffer& data() { return _data; }

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

    Buffer& nulls() {
        return _nulls;
    }

    Buffer& data() {
        return _data;
    }

    DeltaIndex* index() {
        return _index.get();
    }

    bool contains_block(uint32_t bid) const {
        return _index->contains_block(bid);
    }

    uint32_t find_idx(uint32_t rid) {
        return _index->find_idx(rid);
    }

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
