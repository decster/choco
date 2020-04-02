#ifndef CHOCO_MEM_TABLET_GET_H_
#define CHOCO_MEM_TABLET_GET_H_

#include "common.h"
#include "type.h"

namespace choco {


class MemTabletGet {
public:
    MemTabletGet(uint64_t version, const vector<string>& columns);

    void add_row();

private:
    uint64_t _version;
};


} /* namespace choco */

#endif /* CHOCO_MEM_TABLET_GET_H_ */
