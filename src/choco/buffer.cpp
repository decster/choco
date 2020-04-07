#include "buffer.h"

namespace choco {

bool Buffer::init(size_t size, BufferTag tag) {
    if (size > 0) {
        uint8_t* data = (uint8_t*)aligned_malloc(size, 64);
        if (!data) {
            LOG(FATAL) << Format("create buffer size=%zu tag=%u failed, out of memory", size, tag);
            return false;
        }
        _data = data;
        _bsize = size;
    }
    return true;
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
