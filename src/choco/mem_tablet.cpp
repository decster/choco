#include "mem_tablet.h"
#include "mem_tablet_scan.h"
#include "mem_tablet_get.h"

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

Status MemTablet::create_writetx(unique_ptr<WriteTx>& wtx) {
    std::lock_guard<mutex> lg(_vesions_lock);
    wtx.reset(new WriteTx(latest_schema()));
    return Status::OK();
}

Status MemTablet::prepare_writetx(unique_ptr<WriteTx>& wtx) {
    return Status::OK();
}

Status MemTablet::commit(unique_ptr<WriteTx>& wtx, uint64_t version) {
    _sub_tablet->begin_write(latest_schema());
    for (size_t i = 0; i< wtx->batch_size(); i++) {
        auto batch = wtx->get_batch(i);
        PartialRowReader reader(*batch);
        for (size_t j = 0; j<reader.size(); j++) {
            RETURN_NOT_OK(reader.read(j));
            RETURN_NOT_OK(_sub_tablet->apply_partial_row(reader));
        }
    }
    return _sub_tablet->commit_write(version);
}

} /* namespace choco */
