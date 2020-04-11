#include "mem_tablet.h"
#include "mem_tablet_scan.h"
#include "mem_tablet_get.h"
#include "mem_tablet_write.h"

namespace choco {

Status MemTablet::load(const string& dir, uint64_t last_version, unique_ptr<MemTablet>& tablet) {
    return Status::OK();
}

Status MemTablet::create(const string& dir, unique_ptr<Schema>& schema, unique_ptr<MemTablet>& tablet) {
    unique_ptr<MemTablet> ret(new MemTablet());
    ret->_versions.reserve(8);
    ret->_versions.emplace_back(0, std::move(schema));
    ret->_sub_tablet.reset(new MemSubTablet());
    return Status::OK();
}

} /* namespace choco */
