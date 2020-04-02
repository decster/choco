#ifndef CHOCO_BUFFER_H_
#define CHOCO_BUFFER_H_

#include "choco/common.h"

namespace choco {

class Buffer {
public:
    Buffer();
    ~Buffer();

    bool init(uint64_t size, uint32_t tag=0);
    void clear();

    uint8_t* data() {return _data;}
    uint64_t bsize() {return _bsize;}

    template <class T> T* as() { return (T*)_data; }

private:

    uint64_t _bsize = 0;
    uint8_t* _data = nullptr;
};

} /* namespace choco */

#endif /* CHOCO_BUFFER_H_ */
