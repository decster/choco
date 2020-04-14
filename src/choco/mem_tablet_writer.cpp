#include "mem_tablet_writer.h"
#include "mem_tablet.h"
#include "mem_sub_tablet.h"

namespace choco {

MemTabletWriter::MemTabletWriter(shared_ptr<MemTablet>&& tablet, const Schema& write_schema) :
	_tablet(tablet),
	_write_schema(write_schema) {
}

MemTabletWriter::~MemTabletWriter() {

}

Status MemTabletWriter::init() {
	return Status::OK();
}

Status MemTabletWriter::set(unique_ptr<WriteTx>& tx) {
    _wtx.swap(tx);
    return Status::OK();
}

Status MemTabletWriter::commit(uint64_t version) {

	return Status::OK();
}


Status MemTabletWriter::apply() {
    return Status::OK();
}

} /* namespace choco */
