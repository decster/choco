#include "schema.h"

namespace choco {

string ColumnSchema::to_string() const {
    return Format("Column(%s cid=%u %s%s)", name.c_str(), cid, TypeInfo::get(type).name().c_str(), nullable?" null":"");
}

Status ColumnSchema::create(uint32_t cid, const Slice& desc, unique_ptr<ColumnSchema>& cs) {
	//DLOG(INFO) << "check column: " << desc.ToString();
	// name type null
	vector<Slice> fds = desc.split(' ', true);
	if (fds.size() < 2) {
		return Status::InvalidArgument("bad column desc");
	}
	Slice& stype = fds[0];
	Slice& sname = fds[1];
	Type type = Type::Int8;
	if (stype == "int8") {
		type = Type::Int8;
	} else if (stype == "int16") {
		type = Int16;
	} else if (stype == "int32") {
		type = Int32;
	} else if (stype == "int64") {
		type = Int64;
	} else if (stype == "int128") {
		type = Int128;
	} else if (stype == "float32") {
		type = Float32;
	} else if (stype == "flat64") {
		type = Float64;
	} else if (stype == "string") {
		type = String;
	} else {
		return Status::InvalidArgument("bad column desc");
	}
	bool nullable = false;
	if (fds.size() >= 3) {
		if (fds[2] == "null") {
			nullable = true;
		} else {
			return Status::InvalidArgument("bad column desc");
		}
	}
	cs.reset(new ColumnSchema(sname.ToString(), cid, type, nullable));
	return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////

Schema::Schema(vector<ColumnSchema>& columns, uint32_t num_key_column) {
    CHECK_LT(num_key_column, columns.size());
    uint32_t max_cid = 0;
    columns.swap(_columns);
    _num_key_column = num_key_column;
    for (auto& c : _columns) {
        _name_to_col[c.name] = &c;
        max_cid = std::max(c.cid, max_cid);
    }
    _cid_size = max_cid+1;
    _cid_to_col.resize(_cid_size, nullptr);
    for (auto& c : _columns) {
        _cid_to_col[c.cid] = &c;
    }
}

Schema::~Schema() {
}

uint32_t Schema::cid_size() const {
    return _cid_size;
}

const ColumnSchema* Schema::get(const string& name) const {
    auto itr = _name_to_col.find(name);
    if (itr == _name_to_col.end()) {
        return nullptr;
    } else {
        return itr->second;
    }
}

const ColumnSchema* Schema::get(uint32_t cid) const {
    if (cid < _cid_to_col.size()) {
        return _cid_to_col[cid];
    } else {
        return nullptr;
    }
}

Status Schema::create(const Slice& desc, unique_ptr<Schema>& schema) {
	vector<Slice> colstrs = desc.split(',', true);
	if (colstrs.size() < 1) {
		return Status::InvalidArgument("invalid schema description");
	}
	vector<unique_ptr<ColumnSchema>> css(colstrs.size());
	for (size_t i=0;i<colstrs.size();i++) {
		RETURN_NOT_OK(ColumnSchema::create(i+1, colstrs[i], css[i]));
	}
	vector<ColumnSchema> cs;
	cs.reserve(css.size());
	for (size_t i=0;i<css.size();i++) {
		cs.emplace_back(*css[i]);
	}
	schema.reset(new Schema(cs, 1));
	return Status::OK();
}


} /* namespace choco */
