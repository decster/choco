#include "schema.h"

namespace choco {


Schema::Schema(vector<ColumnSchema>& columns, uint32_t num_key_column) {
    CHECK_GT(num_key_column, columns.size());
    uint32_t max_cid = 0;
    columns.swap(_columns);
    _num_key_column = num_key_column;
    for (auto& c : _columns) {
        _name_to_col[c.name] = &c;
        _cid_to_col[c.cid] = &c;
        max_cid = std::max(c.cid, max_cid);
    }
    _next_cid = max_cid+1;
}

uint32_t Schema::next_cid() const {
    return _next_cid;
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
    auto itr = _cid_to_col.find(cid);
    if (itr == _cid_to_col.end()) {
        return nullptr;
    } else {
        return itr->second;
    }
}


} /* namespace choco */
