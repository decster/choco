#include "row_block.h"

namespace choco {

Status ColumnBlock::copy_from(size_t size, size_t esize, Buffer& data, Buffer& nulls) {
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
    memcpy(_data, data.data(), size*esize);
    if (nulls) {
        memcpy(_nulls, nulls.data(), size);
    } else {
        memset(_nulls, 0, size);
    }
    owned = true;
    return Status::OK();
}

void ColumnBlock::clear() {
    if (owned) {
        if (_data) {
            aligned_free(_data);
            _data = nullptr;
        }
        if (_nulls) {
            aligned_free(_nulls);
            _nulls = nullptr;
        }
    }
    owned = false;
}

ColumnBlock::~ColumnBlock() {
    clear();
}


} /* namespace choco */
