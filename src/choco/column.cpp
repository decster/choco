#include "column.h"

namespace choco {

//////////////////////////////////////////////////////////////////////////////

Column::Column(const ColumnSchema& cs, uint64_t version) : _cs(cs) {
    _versions.emplace_back(version);
}



ROColumn::ROColumn(RefPtr<Column>&& column, uint64_t real_version, vector<RefPtr<ColumnPage>>& base, DeltaMode mode, vector<ColumnDelta*>&& deltas) :
    _column(column),
    _real_version(real_version),
    _base(base),
    _mode(mode),
    _deltas(deltas) {
}

//////////////////////////////////////////////////////////////////////////////

template <class T, size_t esize=sizeof(T), bool nullable=false>
class TypedROColumn : public ROColumn {
public:
    virtual ~TypedROColumn() {}

    virtual Status get(uint32_t rid, void * dest) {
        uint32_t bid = rid >> 16;
        DCHECK(bid < _base.size());
        //_base[bid]
        return Status::OK();
    }

    virtual uint64_t hashcode(uint32_t rid) {
        T v = 0;
        Status ret = get(rid, &v);
        DCHECK(ret);
        return HashCode((uint64_t)v);
    }

    virtual bool equals(uint32_t rid, void * rhs) {
        T v = 0;
        Status ret = get(rid, &v);
        DCHECK(ret);
        return v == *(T*)rhs;
    }
};



//////////////////////////////////////////////////////////////////////////////

Status Column::read_at(uint64_t version, unique_ptr<ROColumn>& rc) {
    return Status::OK();
}

} /* namespace choco */
