#ifndef CHOCO_MEM_SUB_TABLET_H_
#define CHOCO_MEM_SUB_TABLET_H_

#include "common.h"
#include "hash_index.h"
#include "column.h"

namespace choco {

class MemSubTablet {
public:

private:
    DISALLOW_COPY_AND_ASSIGN(MemSubTablet);

    mutex _lock;
    HashIndex _index;
    struct VersionInfo {
        uint64_t version;
        uint64_t size;
    };
    vector<VersionInfo> _versions;
    unordered_map<uint32_t, RefPtr<Column>> _columns;
};


} /* namespace choco */

#endif /* CHOCO_MEM_SUB_TABLET_H_ */
