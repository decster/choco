#include "mem_sub_tablet.h"

namespace choco {

Status MemSubTablet::create(uint64_t version, const Schema& schema, unique_ptr<MemSubTablet>& ret) {
	unique_ptr<MemSubTablet> tmp(new MemSubTablet());
	tmp->_versions.reserve(64);
	tmp->_versions.emplace_back(version, 0);
	for (auto& c : schema.columns()) {
		// TODO: support storage_type != c.type
		RefPtr<Column> column(new Column(c, c.type, version), false);
		tmp->_columns.emplace(c.cid, std::move(column));
	}
	tmp.swap(ret);
	return Status::OK();
}

MemSubTablet::MemSubTablet() :
	_index(1<<16) {
	// TODO: check index memory
}

MemSubTablet::~MemSubTablet() {
}

} /* namespace choco */
