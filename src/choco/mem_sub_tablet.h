#ifndef CHOCO_MEM_SUB_TABLET_H_
#define CHOCO_MEM_SUB_TABLET_H_

#include "common.h"
#include "hash_index.h"
#include "column.h"

namespace choco {

class MemSubTablet {
public:
	static Status create(uint64_t version, const Schema& schema, unique_ptr<MemSubTablet>& ret);

    ~MemSubTablet();

private:
    DISALLOW_COPY_AND_ASSIGN(MemSubTablet);

    MemSubTablet();

    mutex _lock;
    HashIndex _index;
    struct VersionInfo {
    	VersionInfo(uint64_t version, uint64_t size) : version(version), size(size) {}
        uint64_t version=0;
        uint64_t size=0;
    };
    vector<VersionInfo> _versions;
    unordered_map<uint32_t, RefPtr<Column>> _columns;
};


} /* namespace choco */

#endif /* CHOCO_MEM_SUB_TABLET_H_ */
