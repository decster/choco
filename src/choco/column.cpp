#include <type_traits>
#include <map>
#include "column.h"

namespace choco {

Status ColumnPage::alloc(size_t size, size_t esize, BufferTag tag) {
    if (_data || _nulls) {
        LOG(FATAL) << "reinit column page";
    }
    Status ret = _data.alloc(size * esize, tag.data());
    if (!ret) {
        return ret;
    }
    _data.set_zero();
    _tag = tag;
    _size = size;
    return Status::OK();
}

Status ColumnPage::set_null(uint32_t idx) {
    if (!_nulls) {
        Status ret = _nulls.alloc(_size, _tag.null());
        if (!ret) {
            return ret;
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


// works for int8/int16/int32/int64/float/double
// TODO: add string support
template <class T, bool Nullable=false, class ST=T>
class TypedColumnReader : public ColumnReader {
public:
    TypedColumnReader(RefPtr<Column>& column, uint64_t version) :
        _column(std::move(column)),
        _base(_column->_base),
        _version(version) {
        _real_version = _version;
    }

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

private:
    RefPtr<Column> _column;
    uint64_t _version;
    uint64_t _real_version;
    vector<RefPtr<ColumnPage>>& _base;
    vector<ColumnDelta*> _deltas;
};



//////////////////////////////////////////////////////////////////////////////

template <class T>
class NullableUpdateType {
public:
    bool& isnull() { return _isnull;}
    T& value() { return _value; }
private:
    bool _isnull;
    T _value = 0;
};

template <class T>
class UpdateType {
public:
    bool& isnull() { return *(bool*)nullptr; /*unused*/ }
    T& value() { return _value; }
private:
    T _value = 0;
};


template <class T, bool Nullable=false, class ST=T, class UT=T>
class TypedColumnWriter : public ColumnWriter {
public:
    TypedColumnWriter(RefPtr<Column>& column) :
        _column(std::move(column)),
        _base(&_column->_base),
        _update_has_null(false) {
    }

    virtual ~TypedColumnWriter() {}

    virtual Status insert(uint32_t rid, const void * value) {
        uint32_t bid = rid >> 16;
        if (bid >= _base->size()) {
            RETURN_NOT_OK(add_page());
            // add one page should be enough
            CHECK(bid < _base->size());
        }
        auto& page = (*_base)[bid];
        uint32_t idx = rid & 0xffff;
        DCHECK(idx * sizeof(T) < page->data().bsize());
        if (Nullable) {
            if (value) {
                if (std::is_same<T, ST>::value) {
                    page->set_not_null(idx);
                    page->data().as<ST>()[idx] = *static_cast<const ST*>(value);
                } else {
                    // TODO: string support
                }
            } else {
                page->set_null(idx);
            }
        } else {
            if (!value) {
                value = _column->schema().default_value_ptr();
            }
            DCHECK_NOTNULL(value);
            if (std::is_same<T, ST>::value) {
                page->data().as<ST>()[idx] = *static_cast<const ST*>(value);
            } else {
                // TODO: string support
            }
        }
        return Status::OK();
    }

    virtual Status update(uint32_t rid, const void * value) {
        DCHECK_LT(rid, _base->size() * Column::BLOCK_SIZE);
        if (Nullable) {
            auto& uv = _updates[rid];
            if (value) {
                uv.isnull() = false;
                if (std::is_same<T, ST>::value) {
                    uv.value() = *(T*)value;
                } else {
                    // TODO: string support
                }
            } else {
                _update_has_null = true;
                uv.isnull() = true;
                if (std::is_same<T, ST>::value) {
                    uv.value() = (T)0;
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
                uv.value() = *static_cast<const ST*>(value);
            } else {
                // TODO: string support
            }
        }
        return Status::OK();
    }

    virtual Status finalize(uint64_t version, RefPtr<Column>& ret) {
        // prepare delta
        size_t nblock = _base->size();
        RefPtr<ColumnDelta> delta = RefPtr<ColumnDelta>::create();
        RETURN_NOT_OK(delta->alloc(
                nblock,
                _updates.size(),
                sizeof(ST),
                BufferTag::delta(_column->schema().cid), _update_has_null));
        DeltaIndex* index = delta->index();
        vector<uint32_t>& block_ends = index->_block_ends;
        Buffer& idxdata = index->_data;
        Buffer& data = delta->data();
        Buffer& nulls = delta->nulls();
        uint32_t cidx = 0;
        uint32_t curbid = 0;
        for (auto& e : _updates) {
            uint32_t rid = e.first;
            uint32_t bid = rid >> 16;
            while (curbid < bid) {
                block_ends[curbid] = cidx;
                curbid++;
            }
            idxdata.as<uint16_t>()[cidx] = rid & 0xffff;
            if (Nullable) {
                bool isnull = e.second.isnull();
                if (isnull) {
                    nulls.as<bool>()[cidx] = true;
                } else {
                    data.as<ST>()[cidx] = e.second.value();
                }
            } else {
                data.as<ST>()[cidx] = e.second.value();
            }
            cidx++;
        }
        _updates.clear();
        RETURN_NOT_OK(add_delta(delta, version));
        ret.swap(_column);
        return Status::OK();
    }

private:
    Status expand_base() {
        size_t added = std::min((size_t)128, _base->capacity());
        size_t new_base_capacity = Padding(_base->capacity() + added, 8);
        // check if version needs expanding too
        size_t new_version_capacity = 0;
        if (_column->_versions.size() == _column->_versions.capacity()) {
            new_version_capacity = Padding(_column->_versions.capacity() + 64, 4);
        }
        // check pool doesn't need expanding
        DCHECK_EQ(_base->size(), _base->capacity());
        DCHECK(_base->capacity() < new_base_capacity);
        RefPtr<Column> cow(new Column(*_column, new_base_capacity, new_version_capacity), false);
        cow.swap(_column);
        return Status::OK();
    }

    Status add_page() {
        if (_base->size() == _base->capacity()) {
            RETURN_NOT_OK(expand_base());
        }
        CHECK_LT(_base->size(), _base->capacity());
        RefPtr<ColumnPage> page = RefPtr<ColumnPage>::create();
        uint32_t cid = _column->schema().cid;
        uint32_t bid = _base->size();
        RETURN_NOT_OK(page->alloc(Column::BLOCK_SIZE, sizeof(ST), BufferTag::base(cid, bid)));
        _base->emplace_back(std::move(page));
        return Status::OK();
    }

    Status expand_delta() {
        size_t new_capacity = Padding(_column->_versions.capacity() + 64, 4);
        RefPtr<Column> cow(new Column(*_column, 0, new_capacity), false);
        cow.swap(_column);
        return Status::OK();
    }

    Status add_delta(RefPtr<ColumnDelta>& delta, uint64_t version) {
        if (_column->_versions.size() == _column->_versions.capacity()) {
            expand_delta();
        }
        CHECK_LT(_base->size(), _base->capacity());
        _column->_versions.emplace_back();
        Column::VersionInfo& vinfo = _column->_versions.back();
        vinfo.version = version;
        vinfo.delta.swap(delta);
        return Status::OK();
    }

    RefPtr<Column> _column;
    vector<RefPtr<ColumnPage>>* _base;

    bool _update_has_null = false;
    typedef typename std::conditional<Nullable, NullableUpdateType<UT>, UpdateType<UT>>::type UpdateMapType;
    std::map<uint32_t, UpdateMapType> _updates;
};

//////////////////////////////////////////////////////////////////////////////

Column::Column(const ColumnSchema& cs, Type storage_type, uint64_t version) :
        _cs(cs),
        _storage_type(storage_type),
        _base_idx(0) {
    _base.reserve(64);
    _versions.reserve(64);
    _versions.emplace_back(version);
}

Column::Column(const Column& rhs, size_t new_base_capacity, size_t new_version_capacity) :
    _cs(rhs._cs),
    _storage_type(rhs._storage_type),
    _base_idx(rhs._base_idx) {
    _base.reserve(std::max(new_base_capacity, rhs._base.capacity()));
    _base.resize(rhs._base.size());
    for (size_t i=0;i<_base.size();i++) {
        _base[i] = rhs._base[i];
    }
    _versions.reserve(std::max(new_version_capacity, rhs._versions.capacity()));
    _versions.resize(rhs._versions.size());
    for (size_t i=0;i<_versions.size();i++) {
        _versions[i] = rhs._versions[i];
    }
}

Status Column::read(uint64_t version, unique_ptr<ColumnReader>& cr) {
    Type type = schema().type;
    bool nullable = schema().nullable;
    RefPtr<Column> pcol(this);
    switch (type) {
    case Int8:
        if (nullable) {
            cr.reset(new TypedColumnReader<int8_t, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<int8_t, false>(pcol, version));
        }
        break;
    case Int16:
        if (nullable) {
            cr.reset(new TypedColumnReader<int16_t, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<int16_t, false>(pcol, version));
        }
        break;
    case Int32:
        if (nullable) {
            cr.reset(new TypedColumnReader<int32_t, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<int32_t, false>(pcol, version));
        }
        break;
    case Int64:
        if (nullable) {
            cr.reset(new TypedColumnReader<int64_t, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<int64_t, false>(pcol, version));
        }
        break;
    case Int128:
        if (nullable) {
            cr.reset(new TypedColumnReader<int128_t, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<int128_t, false>(pcol, version));
        }
        break;
    case Float32:
        if (nullable) {
            cr.reset(new TypedColumnReader<float, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<float, false>(pcol, version));
        }
        break;
    case Float64:
        if (nullable) {
            cr.reset(new TypedColumnReader<double, true>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<double, false>(pcol, version));
        }
        break;
    case String:
        if (nullable) {
            cr.reset(new TypedColumnReader<Slice, true, int32_t>(pcol, version));
        } else {
            cr.reset(new TypedColumnReader<Slice, false, int32_t>(pcol, version));
        }
        break;
    default:
        LOG(FATAL) << "unsupported type for ColumnReader";
    }
    return Status::OK();
}

Status Column::write(unique_ptr<ColumnWriter>& cw) {
    Type type = schema().type;
    bool nullable = schema().nullable;
    RefPtr<Column> pcol(this);
    switch (type) {
    case Int8:
        if (nullable) {
            cw.reset(new TypedColumnWriter<int8_t, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<int8_t>(pcol));
        }
        break;
    case Int16:
        if (nullable) {
            cw.reset(new TypedColumnWriter<int16_t, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<int16_t>(pcol));
        }
        break;
    case Int32:
        if (nullable) {
            cw.reset(new TypedColumnWriter<int32_t, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<int32_t>(pcol));
        }
        break;
    case Int64:
        if (nullable) {
            cw.reset(new TypedColumnWriter<int64_t, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<int64_t>(pcol));
        }
        break;
    case Int128:
        if (nullable) {
            cw.reset(new TypedColumnWriter<int128_t, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<int128_t>(pcol));
        }
        break;
    case Float32:
        if (nullable) {
            cw.reset(new TypedColumnWriter<float, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<float>(pcol));
        }
        break;
    case Float64:
        if (nullable) {
            cw.reset(new TypedColumnWriter<double, true>(pcol));
        } else {
            cw.reset(new TypedColumnWriter<double>(pcol));
        }
        break;
    case String:
        // TODO:
        LOG(FATAL) << "unsupported type for ColumnWriter";
        break;
    default:
        LOG(FATAL) << "unsupported type for ColumnWriter";
    }
    return Status::OK();
}

Status Column::delta_compaction(RefPtr<Column>& result, uint64_t to_version) {
    return Status::OK();
}

} /* namespace choco */
