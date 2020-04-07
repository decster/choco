#include <type_traits>
#include <map>
#include "column.h"

namespace choco {

Status ColumnPage::init(size_t size, size_t esize, BufferTag tag) {
    if (_data || _nulls) {
        LOG(FATAL) << "reinit column page";
    }
    bool ok = _data.init(size * esize, tag.data());
    if (!ok) {
        return Status::OOM("init column page");
    }
    _data.set_zero();
    _tag = tag;
    _size = size;
    return Status::OK();
}

Status ColumnPage::set_null(uint32_t idx) {
    if (!_nulls) {
        bool ok = _nulls.init(_size, _tag.null());
        if (!ok) {
            return Status::OOM("init column page nulls");
        }
        _nulls.set_zero();
    }
    _nulls.as<bool>()[idx] = true;
    return Status::OK();
}

Status ColumnPage::set_not_null(uint32_t idx) {
    if (_nulls) {
        _nulls.as<bool>()[idx] = false;
    }
    return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////

ColumnReader::ColumnReader(RefPtr<Column>& column, uint64_t real_version, vector<ColumnPage*>& base, vector<ColumnDelta*>& deltas) :
    _column(std::move(column)),
    _real_version(real_version),
    _base(base),
    _deltas(std::move(deltas)) {
}

// works for int8/int16/int32/int64/float/double
// TODO: add string support
template <class T, class ST=T, bool Nullable=false>
class TypedColumnReader : public ColumnReader {
public:
    TypedColumnReader(RefPtr<Column>& column, uint64_t real_version, vector<ColumnPage*>& base, vector<ColumnDelta*>& deltas) :
        ColumnReader(column, real_version, base, deltas) {}

    virtual ~TypedColumnReader() {}

    inline void get_cell(const uint32_t rid, bool& isnull, ST*& pcell) const {
        // TODO: test perf move delta to template
        for (ssize_t i=_deltas.size()-1;i>=0;i--) {
            ColumnDelta* pdelta = _deltas[i];
            uint32_t pos = pdelta->find_idx(rid);
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
        DCHECK(idx * sizeof(ST) < _base[bid]->data().bsize());
        if (Nullable) {
            isnull = _base[bid]->is_null(idx);
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

ColumnWriter::ColumnWriter(Column* column) : _column(column), _base(column->_base) {}

template <class T>
struct NullableUpdateType {
    bool isnull;
    T value = 0;
};


template <class T, class ST=T, class UT=ST, bool Nullable=false>
class TypedColumnWriter : public ColumnWriter {
public:
    TypedColumnWriter(Column* column) : ColumnWriter(column), _update_has_null(false) {}

    virtual ~TypedColumnWriter() {}

    virtual Status reserve(size_t size) {
        if (size <= _column->_capacity) {
            return Status::OK();
        }
        size_t psize = (size + Column::BLOCK_SIZE - 1) / Column::BLOCK_SIZE;
        size_t old_size = _base.size();
        size_t required = psize - old_size;
        vector<RefPtr<ColumnPage>> rcs(required);
        for (size_t i=0;i<rcs.size();i++) {
            rcs[i] = RefPtr<ColumnPage>::create();
            Status ret = rcs[i]->init(sizeof(ST)*Column::BLOCK_SIZE, 0, BufferTag::base(_column->schema().cid, (uint32_t)(old_size+i)));
            if (!ret) {
                return ret;
            }
        }
        {
            std::lock_guard<mutex> lg(_column->_lock);
            _base.resize(psize);
            for (size_t i=0;i<required;i++) {
                _base[old_size+i].swap(rcs[i]);
            }
        }
        _column->_capacity = _base.size() * Column::BLOCK_SIZE;
        return Status::OK();
    }

    virtual Status insert(uint32_t rid, void * value) {
        DCHECK_LT(rid, _column->_capacity);
        uint32_t bid = rid >> 16;
        DCHECK(bid < _base.size());
        uint32_t idx = rid & 0xffff;
        DCHECK(idx * sizeof(T) < _base[bid]->data().bsize());
        if (Nullable) {
            if (value) {
                if (std::is_same<T, ST>::value) {
                    _base[bid]->data().as<ST>()[idx] = *value;
                } else {
                    // TODO: string support
                }
            } else {
                _base[bid]->set_null(idx);
            }
        } else {
            if (!value) {
                value = _column->schema().default_value_ptr();
            }
            DCHECK_NOTNULL(value);
            if (std::is_same<T, ST>::value) {
                _base[bid]->data().as<ST>()[idx] = *static_cast<ST*>(value);
            } else {
                // TODO: string support
            }
        }
        return Status::OK();
    }

    virtual Status update(uint32_t rid, void * value) {
        DCHECK_LT(rid, _column->_capacity);
        if (Nullable) {
            auto& uv = _updates[rid];
            if (value) {
                uv.isnull = false;
                if (std::is_same<T, ST>::value) {
                    uv.value = *(T*)value;
                } else {
                    // TODO: string support
                }
            } else {
                _update_has_null = true;
                uv.isnull = true;
                if (std::is_same<T, ST>::value) {
                    uv.value = (T)0;
                } else {
                    // TODO: string support
                }
            }
        } else {
            auto& uv = _updates[rid];
            if (!value) {
                value = _column->schema().default_value_ptr();
            }
            DCHECK_NOTNULL(value);
            if (std::is_same<T, ST>::value) {
                uv.value = *(T*)value;
            } else {
                // TODO: string support
            }
        }
        return Status::OK();
    }

    virtual Status finalize() {
        // prepare delta
        RefPtr<ColumnDelta> delta = RefPtr<ColumnDelta>::create();
        Status ret = delta->init(
                _base.size(),
                _updates.size(),
                sizeof(ST),
                BufferTag::delta(_column->schema().cid), _update_has_null);
        if (!ret) {
            return ret;
        }
        DeltaIndex* index = delta->index();
        Buffer& data = delta->data();
        Buffer& nulls = delta->nulls();
        uint32_t cidx = 0;
        for (auto& e : _updates) {
            index->append_rid(cidx, e.first);
            if (Nullable) {
                bool isnull = nulls.as<bool>[cidx] = e.second.isnull;
                if (!isnull) {
                    data.as<ST>[cidx] = e.second.value;
                }
            } else {
                data.as<ST>[cidx] = e.second;
            }
            cidx++;
        }
        index->finalize(cidx);
        _updates.clear();
        _delta.swap(delta);
        return Status::OK();
    }

private:
    bool _update_has_null = false;
    std::map<uint32_t, UT> _updates;
};

//////////////////////////////////////////////////////////////////////////////

Column::Column(const ColumnSchema& cs, uint64_t version) : _cs(cs), _storage_type(cs.type), _capacity(0) {
    _versions.emplace_back(version);
}

Status Column::read_at(uint64_t version, unique_ptr<ColumnReader>& cr) {
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
            cr.reset(new TypedColumnReader<int8_t, int8_t, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<int8_t, int8_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int16:
        if (nullable) {
            cr.reset(new TypedColumnReader<int16_t, int16_t, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<int16_t, int16_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int32:
        if (nullable) {
            cr.reset(new TypedColumnReader<int32_t, int32_t, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<int32_t, int32_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int64:
        if (nullable) {
            cr.reset(new TypedColumnReader<int64_t, int64_t, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<int64_t, int64_t, false>(pcol, version, base, deltas));
        }
        break;
    case Int128:
        if (nullable) {
            cr.reset(new TypedColumnReader<int128_t, int128_t, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<int128_t, int128_t, false>(pcol, version, base, deltas));
        }
        break;
    case Float32:
        if (nullable) {
            cr.reset(new TypedColumnReader<float, float, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<float, float, false>(pcol, version, base, deltas));
        }
        break;
    case Float64:
        if (nullable) {
            cr.reset(new TypedColumnReader<double, double, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<double, double, false>(pcol, version, base, deltas));
        }
        break;
    case String:
        if (nullable) {
            cr.reset(new TypedColumnReader<Slice, int32_t, true>(pcol, version, base, deltas));
        } else {
            cr.reset(new TypedColumnReader<Slice, int32_t, false>(pcol, version, base, deltas));
        }
        break;
    default:
        LOG(FATAL) << "unsupported type for ColumnReader";
    }
    return Status::OK();
}


void Column::prepare_writer() {
    Type type = schema().type;
    bool nullable = schema().nullable;
    switch (type) {
    case Int8:
        break;
    }
}

Status Column::start_write(unique_ptr<ColumnWriter>& cw, bool wait) {
    return Status::OK();
}

Status Column::cancel_write(unique_ptr<ColumnWriter>& cw) {
    return Status::OK();
}

Status Column::finish_write(unique_ptr<ColumnWriter>& cw, uint64_t version) {
    return Status::OK();
}

Status Column::delta_compaction(RefPtr<Column>& result, uint64_t to_version) {
    return Status::OK();
}

} /* namespace choco */
