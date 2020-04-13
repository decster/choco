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

// for test with demo table (int32 id,int32 uv,int32 pv,int8 city null)
Status MemTabletWriter::insert(int32_t key, int32_t uv, int32_t pv, int8_t * pcity) {
	return Status::OK();
}

Status MemTabletWriter::update_uv(int32_t key, int32_t uv) {
	return Status::OK();
}

Status MemTabletWriter::update_pv(int32_t key, int32_t pv) {
	return Status::OK();
}

Status MemTabletWriter::update_city(int32_t key, int8_t* pcity) {
	return Status::OK();

}


Status MemTabletWriter::commit(uint64_t version) {
	return Status::OK();
}


} /* namespace choco */
