#include "column_delta.h"

namespace choco {

size_t DeltaIndex::memory() const {
    return _data.bsize() + _block_ends.size() * sizeof(uint32_t);
}

uint32_t DeltaIndex::find_idx(uint32_t rid) {
    if (!_data) {
        return npos;
    }
    uint32_t bid = rid >> 16;
    if (bid >= _block_ends.size()) {
        return npos;
    }
    // TODO: use SIMD
    uint32_t start = bid > 0 ? _block_ends[bid-1]:0;
    uint32_t end = _block_ends[bid];
    if (start == end) {
        return npos;
    }
    uint16_t * astart = _data.as<uint16_t>() + start;
    uint16_t * aend = _data.as<uint16_t>() + end;
    uint32_t bidx = rid & 0xffff;
    uint16_t* pos = std::lower_bound(astart, aend, bidx);
    if ((pos != aend) && (*pos == bidx)) {
        return pos - _data.as<uint16_t>();
    } else {
        return npos;
    }
}

//////////////////////////////////////////////////////////////////////////////

size_t ColumnDelta::memory() const {
    return _index->memory() + _nulls.bsize() + _data.bsize();
}

Status ColumnDelta::alloc(size_t nblock, size_t size, size_t esize, BufferTag tag, bool has_null) {
    if (_data || _nulls) {
        LOG(FATAL) << "reinit column delta";
    }
    _index = RefPtr<DeltaIndex>::create();
    _index->_block_ends.resize(nblock, 0);
    Status ret = _index->_data.alloc(size*sizeof(uint16_t), tag.index());
    if (!ret) {
        return ret;
    }
    ret = _data.alloc(size * esize, tag.data());
    if (!ret) {
        return ret;
    }
    if (has_null) {
        ret = _nulls.alloc(size, tag.null());
        if (!ret) {
            _data.clear();
            return Status::OOM("init column delta nulls");
        }
        _nulls.set_zero();
    }
    _data.set_zero();
    _tag = tag;
    _size = size;
    return Status::OK();
}


Status ColumnDelta::create_for_compaction(RefPtr<ColumnDelta>& ret) {
    // TODO: compaction
    return Status::NotSupported("not implemented");
}

} /* namespace choco */
