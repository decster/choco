#ifndef CHOCO_BUFFER_H_
#define CHOCO_BUFFER_H_

#include "common.h"

namespace choco {

struct BufferTag {
    BufferTag(uint64_t tag) : tag(tag) {}

    static BufferTag base(uint32_t cid, uint32_t bid) { return  BufferTag( (((uint64_t)cid) << 32) | (((uint64_t)bid) << 16)); };

    static BufferTag delta(uint32_t cid) { return BufferTag( (((uint64_t)cid) << 32) | (((uint64_t)0xffff) << 16) ); }

    BufferTag null() { return BufferTag(tag | 1); }
    BufferTag data() { return BufferTag(tag | 2); }
    BufferTag index() { return BufferTag(tag | 4); }
    BufferTag pool() { return BufferTag(tag | 8); }

    uint64_t tag;
};


class Buffer {
public:
    Buffer() = default;
    ~Buffer();

    Status alloc(size_t size, BufferTag tag=0);
    void clear();

    void set_zero();

    operator bool() const {return _data != nullptr;}
    const uint8_t* data() const {return _data;}
    uint8_t* data() {return _data;}
    size_t bsize() const {return _bsize;}

    template <class T> T* as() { return (T*)_data; }

    template <class T> const T* as() const { return (const T*)_data; }

private:

    size_t _bsize = 0;
    uint8_t* _data = nullptr;
};

} /* namespace choco */

#endif /* CHOCO_BUFFER_H_ */
