#include "column_delta.h"

namespace choco {

void DeltaIndex::append_rid(uint32_t cidx, uint32_t rid) {
    uint32_t bid = rid >> 16;
    uint32_t idx = rid & 0xffff;
    _data.as<uint16_t>()[cidx] = idx;
    _block_ends[bid] = cidx+1;
}

void DeltaIndex::finalize(uint32_t cidx) {
    for (size_t i=1;i<_block_ends.size();i++) {
        if(_block_ends[i] == 0) {
            _block_ends[i] =
        }
    }
}


//////////////////////////////////////////////////////////////////////////////

Status ColumnDelta::init(size_t nblock, size_t size, size_t esize, BufferTag tag, bool has_null) {
    if (_data || _nulls) {
        LOG(FATAL) << "reinit column delta";
    }
    _index = RefPtr<DeltaIndex>::create();
    _index->_block_ends.resize(nblock);
    for (size_t i=0;i<_index->_block_ends.size();i++) {
        _index->_block_ends[i] = 0;
    }
    bool ok = _index->_data.init(size*sizeof(uint16_t), tag.index());
    if (!ok) {
        return Status::OOM("init column delta index");
    }
    ok = _data.init(size * esize, tag.data());
    if (!ok) {
        return Status::OOM("init column delta data");
    }
    if (has_null) {
        ok = _nulls.init(size, tag.null());
        if (!ok) {
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


Status ColumnDelta::create_for_undo(RefPtr<ColumnDelta>& ret) {
    // TODO: compaction
    return Status::NotSupported("not implemented");
}

} /* namespace choco */
