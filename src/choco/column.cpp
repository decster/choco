#include <type_traits>
#include "column.h"

namespace choco {

//////////////////////////////////////////////////////////////////////////////

static RefPtr<ColumnPage> alloc_page(Type storage_type, size_t size) {
    return RefPtr<ColumnPage>();
}

Column::Column(const ColumnSchema& cs, uint64_t version) : _cs(cs), _storage_type(cs.type) {
    _versions.emplace_back(version);
}

Status Column::reserve(size_t size) {
    size_t psize = (size + BLOCK_SIZE - 1) >> 16;
    {
        std::lock_guard<mutex> lg(_lock);
        if (psize > _base.size()) {
            size_t required = psize - _base.size();
            vector<RefPtr<ColumnPage>> rcs(required);
            for (size_t i=0;i<rcs.size();i++) {
                rcs[i] = alloc_page(_storage_type, BLOCK_SIZE);
                if (!rcs[i]) {
                    return Status::OOM("Not enough memory for ColumnPage");
                }
            }
        }
    }
    return Status::OK();
}

void Column::set(uint32_t rid, void * value) {

}


//////////////////////////////////////////////////////////////////////////////

ROColumn::ROColumn(RefPtr<Column>& column, uint64_t real_version, vector<ColumnPage*>& base, vector<ColumnDelta*>& deltas) :
    _column(std::move(column)),
    _real_version(real_version),
    _base(base),
    _deltas(std::move(deltas)) {
}

//////////////////////////////////////////////////////////////////////////////


// works for int8/int16/int32/int64/float/double
// not work for string
template <class T, class ST=T, bool Nullable=false>
class TypedROColumn : public ROColumn {
public:
    TypedROColumn(RefPtr<Column>& column, uint64_t real_version, vector<ColumnPage*>& base, vector<ColumnDelta*>& deltas) :
        ROColumn(column, real_version, base, deltas) {}

    virtual ~TypedROColumn() {}

    inline void get_cell(const uint32_t rid, bool& isnull, ST*& pcell) const {
        // TODO: test perf move delta to template
        for (ssize_t i=_deltas.size()-1;i>=0;i--) {
            ColumnDelta* pdelta = _deltas[i];
            uint32_t pos = pdelta->index()->find_idx(rid);
            if (pos != (uint32_t)-1) {
                if (Nullable) {
                    isnull = pdelta->nulls() && pdelta->nulls().as<bool>()[pos];
                    if (!isnull) {
                        pcell = &(pdelta->data().as<ST>()[pos]);
                    }
                } else {
                    isnull = false;
                    pcell = &(pdelta->data().as<ST>()[pos]);
                }
            }
        }
        uint32_t bid = rid >> 16;
        DCHECK(bid < _base.size());
        uint32_t idx = rid & 0xffff;
        DCHECK(idx * sizeof(T) < _base[bid]->data().bsize());
        if (Nullable) {
            isnull = _base[bid]->nulls() && _base[bid]->nulls().as<bool>()[idx];
            if (!isnull) {
                pcell = &(_base[bid]->data().as<ST>()[idx]);
            }
        } else {
            isnull = false;
            pcell = &(_base[bid]->data().as<ST>()[idx]);
        }
    }

    virtual bool get(const uint32_t rid, void * dest) const {
        bool isnull;
        ST* pcell;
        get_cell(rid, isnull, pcell);
        if (isnull) {
            return false;
        } else {
            if (std::is_same<T, ST>::value) {
                *(ST*)dest = *pcell;
            }
            return true;
        }
    }

    virtual uint64_t hashcode(const uint32_t rid) const {
        bool isnull;
        ST* pcell;
        get_cell(rid, isnull, pcell);
        if (isnull) {
            return 0;
        } else {
            if (std::is_same<T, ST>::value) {
                return HashCode(*pcell);
            } else {
                // TODO: support dict
                return 0;
            }
        }
    }

    virtual bool equals(const uint32_t rid, void * rhs) const {
        bool isnull;
        ST* pcell;
        get_cell(rid, isnull, pcell);
        if (isnull) {
            return rhs == nullptr;
        } else {
            if (std::is_same<T, ST>::value) {
                return *(ST*)rhs == *pcell;
            } else {
                // TODO: support dict
                return false;
            }
        }
    }
};


//////////////////////////////////////////////////////////////////////////////

Status Column::read_at(uint64_t version, unique_ptr<ROColumn>& rc) {
    Type type = schema().type;
    bool nullable = schema().nullable;
    RefPtr<Column> pcol(this);
    uint64_t real_version = version;
    vector<ColumnPage*> base;
    vector<ColumnDelta*> deltas;
    {
        // TODO:
        std::lock_guard<mutex> lg(_lock);
        base.resize(_base.size());
        memcpy(base.data(), _base.data(), sizeof(ColumnPage*)*_base.size());
    }
    switch (type) {
    case Int8:
        if (nullable) {
            rc.reset(new TypedROColumn<int8_t, int8_t, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<int8_t, int8_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int16:
        if (nullable) {
            rc.reset(new TypedROColumn<int16_t, int16_t, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<int16_t, int16_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int32:
        if (nullable) {
            rc.reset(new TypedROColumn<int32_t, int32_t, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<int32_t, int32_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int64:
        if (nullable) {
            rc.reset(new TypedROColumn<int64_t, int64_t, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<int64_t, int64_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int128:
        if (nullable) {
            rc.reset(new TypedROColumn<int128_t, int128_t, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<int128_t, int128_t, false>(pcol, version, base, deltas));
        }
        break;
    case Float32:
        if (nullable) {
            rc.reset(new TypedROColumn<float, float, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<float, float, false>(pcol, version, base, deltas));
        }
        break;
    case Float64:
        if (nullable) {
            rc.reset(new TypedROColumn<double, double, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<double, double, false>(pcol, version, base, deltas));
        }
        break;
    case String:
        if (nullable) {
            rc.reset(new TypedROColumn<Slice, int32_t, true>(pcol, version, base, deltas));
        } else {
            rc.reset(new TypedROColumn<Slice, int32_t, false>(pcol, version, base, deltas));
        }
        break;
    default:
        LOG(FATAL) << "unsupported type for ROColumn";
    }
    return Status::OK();
}

} /* namespace choco */
