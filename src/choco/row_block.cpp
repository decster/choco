#include "row_block.h"

namespace choco {

Status ColumnBlock::alloc(size_t size, size_t esize) {
    if (_owned_size == size) {
        return Status::OK();
    }
    clear();
    _data = (uint8_t*)aligned_malloc(size*esize, size*esize>=4096?4096:64);
    if (!_data) {
        return Status::OOM("OOM when allocating column block");
    }
    _nulls = (uint8_t*)aligned_malloc(size, size>=4096?4096:64);
    if (!_nulls) {
        aligned_free(_data);
        _data = nullptr;
        return Status::OOM("OOM when allocating column block");
    }
    _owned_size = size;
    return Status::OK();
}

Status ColumnBlock::copy_from(size_t size, size_t esize, Buffer& data, Buffer& nulls) {
    RETURN_NOT_OK(alloc(size, esize));
    memcpy(_data, data.data(), size*esize);
    if (nulls) {
        memcpy(_nulls, nulls.data(), size);
    } else {
        memset(_nulls, 0, size);
    }
    return Status::OK();
}

void ColumnBlock::clear() {
    if (_owned_size > 0) {
        if (_data) {
            aligned_free(_data);
            _data = nullptr;
        }
        if (_nulls) {
            aligned_free(_nulls);
            _nulls = nullptr;
        }
    }
    _owned_size = 0;
}

ColumnBlock::~ColumnBlock() {
    clear();
}


} /* namespace choco */
