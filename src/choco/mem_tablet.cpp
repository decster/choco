#include "mem_tablet.h"
#include "mem_tablet_scan.h"
#include "mem_tablet_get.h"
#include "mem_tablet_writer.h"

namespace choco {

Status MemTablet::load(const string& dir, uint64_t last_version, shared_ptr<MemTablet>& tablet) {
    return Status::OK();
}

Status MemTablet::create(const string& dir, unique_ptr<Schema>& schema, shared_ptr<MemTablet>& tablet) {
    uint64_t version = 0;
    unique_ptr<MemSubTablet> st;
    unordered_map<uint32_t, RefPtr<Column>> columns;
    RETURN_NOT_OK(MemSubTablet::create(version, *schema, st));
    shared_ptr<MemTablet> ret(new MemTablet());
    ret->_versions.reserve(8);
    ret->_versions.emplace_back(version, schema);
    ret->_sub_tablet.swap(st);
    tablet.swap(ret);
    return Status::OK();
}

MemTablet::MemTablet() {

}

MemTablet::~MemTablet() {

}

const Schema& MemTablet::latest_schema() const {
	DCHECK_GT(_versions.size(), 0);
	return *(_versions.back().schema);
}


Status MemTablet::scan(unique_ptr<ScanSpec>& spec, unique_ptr<MemTabletScan>& scan) {
	return Status::OK();
}

Status MemTablet::write(unique_ptr<MemTabletWriter>& writer) {
	unique_ptr<MemTabletWriter> ret(new MemTabletWriter(
			shared_from_this(),
			latest_schema()
			));

	ret.swap(writer);
	return Status::OK();
}


} /* namespace choco */
