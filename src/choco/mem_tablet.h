#ifndef CHOCO_MEM_TABLET_H_
#define CHOCO_MEM_TABLET_H_

#include "common.h"
#include "mem_sub_tablet.h"
#include "write_tx.h"

namespace choco {

class ScanSpec;
class MemTabletScan;

class MemTablet : std::enable_shared_from_this<MemTablet> {
public:
    static Status load(const string& dir, uint64_t last_version, shared_ptr<MemTablet>& tablet);
    static Status create(const string& dir, unique_ptr<Schema>& schema, shared_ptr<MemTablet>& tablet);

    ~MemTablet();

    const Schema& latest_schema() const;

    Status scan(unique_ptr<ScanSpec>& spec, unique_ptr<MemTabletScan>& scan);

    Status create_writetx(unique_ptr<WriteTx>& wtx);
    Status prepare_writetx(unique_ptr<WriteTx>& wtx);
    Status commit(unique_ptr<WriteTx>& wtx, uint64_t version);

private:
    friend class MemTabletScan;
    DISALLOW_COPY_AND_ASSIGN(MemTablet);

    MemTablet();

    mutex _vesions_lock;
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
