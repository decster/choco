#ifndef CHOCO_MEM_TABLET_H_
#define CHOCO_MEM_TABLET_H_

#include "common.h"
#include "mem_sub_tablet.h"
//#include "mem_tablet_scan.h"

namespace choco {

class ScanSpec;
class MemTabletScan;
class MemTabletGet;
class MemTabletWrite;


class MemTablet {
public:
    static Status load(const string& dir, uint64_t last_version, unique_ptr<MemTablet>& tablet);
    static Status create(const string& dir, unique_ptr<Schema>& schema, unique_ptr<MemTablet>& tablet);

    ~MemTablet() = default;

    Status scan(unique_ptr<ScanSpec>& spec, unique_ptr<MemTabletScan>& scan);
    Status get(unique_ptr<MemTabletGet>& get);
    Status create_write(unique_ptr<MemTabletWrite>& write);
    Status prepare_write(unique_ptr<MemTabletWrite>& write);
    Status commit_write(unique_ptr<MemTabletWrite>& write, uint64_t version);

private:
    MemTablet() = default;

    mutex _lock;
    struct VersionInfo {
        VersionInfo(uint64_t version, unique_ptr<Schema>& schema) :
            version(version), schema(std::move(schema)) {
        }
        uint64_t version;
        unique_ptr<Schema> schema;
    };
    vector<VersionInfo> _versions;
    // TODO: support multiple subtablet in future
    unique_ptr<MemSubTablet> _sub_tablet;
};


} /* namespace choco */

#endif /* CHOCO_MEM_TABLET_H_ */
