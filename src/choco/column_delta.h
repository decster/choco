#ifndef CHOCO_COLUMN_DELTA_H_
#define CHOCO_COLUMN_DELTA_H_

#include "common.h"
#include "type.h"
#include "buffer.h"

namespace choco {

class DeltaIndex : public RefCounted {
public:
    DeltaIndex() = default;
private:
    vector<uint32_t> _block_starts;
    Buffer _data;

//    struct alignas(64) BlockIdx {
//        uint16_t samples[32];
//    };
//    vector<BlockIdx> _block_idxs;
};


class ColumnDelta : public RefCounted {
public:
    ColumnDelta() = default;
private:
    RefPtr<DeltaIndex> _index;
    Buffer _nulls;
    Buffer _data;
};



} /* namespace choco */

#endif /* CHOCO_COLUMN_DELTA_H_ */
