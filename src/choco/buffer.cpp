#include "buffer.h"

namespace choco {

Status Buffer::alloc(size_t size, BufferTag tag) {
    if (size > 0) {
        uint8_t* data = (uint8_t*)aligned_malloc(size, size>=4096?4096:64);
        if (!data) {
            return Status::OOM(Format("alloc buffer size=%zu tag=%016lx failed", size, tag.tag));
        }
        _data = data;
        _bsize = size;
    }
    return Status::OK();
}

void Buffer::clear() {
    if (_data) {
        aligned_free(_data);
        _data = nullptr;
        _bsize = 0;
    }
}

void Buffer::set_zero() {
    if (_data) {
        memset(_data, 0, _bsize);
    }
}


Buffer::~Buffer() {
    clear();
}

} /* namespace choco */
