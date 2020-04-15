#include <type_traits>
#include <map>
#include "column.h"

namespace choco {

size_t ColumnPage::memory() const {
    return _data.bsize() + _nulls.bsize();
}

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
    TypedColumnReader(RefPtr<Column>& column, uint64_t version, uint64_t real_version, vector<ColumnDelta*>& deltas) :
        _column(std::move(column)),
        _base(&_column->_base),
        _version(version),
        _real_version(real_version),
        _deltas(std::move(deltas)) {
    }

    virtual ~TypedColumnReader() {}

    virtual const void * get(const uint32_t rid) const {
        for (ssize_t i=_deltas.size()-1;i>=0;i--) {
            ColumnDelta* pdelta = _deltas[i];
            uint32_t pos = pdelta->find_idx(rid);
            if (pos != DeltaIndex::npos) {
                if (Nullable) {
                    bool isnull = pdelta->nulls() && pdelta->nulls().as<bool>()[pos];
                    if (isnull) {
                        return nullptr;
                    } else {
                        return &(pdelta->data().as<ST>()[pos]);
                    }
                } else {
                    return &(pdelta->data().as<ST>()[pos]);
                }
            }
        }
        uint32_t bid = rid >> 16;
        DCHECK(bid < _base->size());
        uint32_t idx = rid & 0xffff;
        DCHECK(idx * sizeof(ST) < (*_base)[bid]->data().bsize());
        if (Nullable) {
            bool isnull = (*_base)[bid]->is_null(idx);
            if (isnull) {
                return nullptr;
            } else {
                return &((*_base)[bid]->data().as<ST>()[idx]);
            }
        } else {
            return &((*_base)[bid]->data().as<ST>()[idx]);
        }
    }

    virtual string to_string() const {
        return Format("%s version=%zu rversion=%zu ndelta=%zu",
                _column->to_string().c_str(),
                _version,
                _real_version,
                _deltas.size());
    }

private:
    RefPtr<Column> _column;
    uint64_t _version;
    uint64_t _real_version;
    vector<RefPtr<ColumnPage>>* _base;
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
        _column->capture_latest(_deltas);
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
        _num_insert++;
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
        _num_update++;
        return Status::OK();
    }

    virtual Status finalize(uint64_t version) {
        if (_updates.size() == 0) {
            // insert(append) only
            return Status::OK();
        }
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
        while (curbid < nblock) {
            block_ends[curbid] = cidx;
            curbid++;
        }
        _updates.clear();
        RETURN_NOT_OK(add_delta(delta, version));
        return Status::OK();
    }

    virtual Status get_new_column(RefPtr<Column>& ret) {
        if (ret != _column) {
            ret.swap(_column);
            _column.reset();
        }
        return Status::OK();
    }

    virtual const void * get(const uint32_t rid) const {
        for (ssize_t i=_deltas.size()-1;i>=0;i--) {
            ColumnDelta* pdelta = _deltas[i];
            uint32_t pos = pdelta->find_idx(rid);
            if (pos != DeltaIndex::npos) {
                if (Nullable) {
                    bool isnull = pdelta->nulls() && pdelta->nulls().as<bool>()[pos];
                    if (isnull) {
                        return nullptr;
                    } else {
                        return &(pdelta->data().as<ST>()[pos]);
                    }
                } else {
                    return &(pdelta->data().as<ST>()[pos]);
                }
            }
        }
        uint32_t bid = rid >> 16;
        DCHECK(bid < _base->size());
        uint32_t idx = rid & 0xffff;
        DCHECK(idx * sizeof(ST) < (*_base)[bid]->data().bsize());
        if (Nullable) {
            bool isnull = (*_base)[bid]->is_null(idx);
            if (isnull) {
                return nullptr;
            } else {
                return &((*_base)[bid]->data().as<ST>()[idx]);
            }
        } else {
            return &((*_base)[bid]->data().as<ST>()[idx]);
        }
    }

    // borrow a virtual function slot to do typed hash
    virtual uint64_t hashcode(const void * data) const {
        if (std::is_same<T, ST>::value) {
            return HashCode(*(const T*)data);
        } else {
            // TODO: support SString hash
            return 0;
        }
    }

    virtual bool equals(const uint32_t rid, const void * rhs) const {
        for (ssize_t i=_deltas.size()-1;i>=0;i--) {
            ColumnDelta* pdelta = _deltas[i];
            uint32_t pos = pdelta->find_idx(rid);
            if (pos != DeltaIndex::npos) {
                if (Nullable) {
                    bool isnull = pdelta->nulls() && pdelta->nulls().as<bool>()[pos];
                    if (isnull) {
                        return rhs == nullptr;
                    } else {
                        return (pdelta->data().as<T>()[pos]) == *(const T*)rhs;
                    }
                } else {
                    return (pdelta->data().as<T>()[pos]) == *(const T*)rhs;
                }
            }
        }
        uint32_t bid = rid >> 16;
        DCHECK(bid < _base->size());
        uint32_t idx = rid & 0xffff;
        DCHECK(idx * sizeof(ST) < (*_base)[bid]->data().bsize());
        if (Nullable) {
            bool isnull = (*_base)[bid]->is_null(idx);
            if (isnull) {
                return rhs == nullptr;
            } else {
                return ((*_base)[bid]->data().as<T>()[idx]) == *(const T*)rhs;
            }
        } else {
            DCHECK_NOTNULL(rhs);
            return ((*_base)[bid]->data().as<T>()[idx]) == *(const T*)rhs;
        }
    }

    virtual string to_string() const {
        return Format("%s insert:%zu update:%zu",
                _column->to_string().c_str(),
                _num_insert,
                _num_update);
    }

private:
    Status expand_base() {
        size_t added = std::min((size_t)256, _base->capacity());
        size_t new_base_capacity = Padding(_base->capacity() + added, 8);
        // check if version needs expanding too
        size_t new_version_capacity = 0;
        if (_column->_versions.size() == _column->_versions.capacity()) {
            new_version_capacity = Padding(_column->_versions.capacity() + 64, 4);
        }
        // check pool doesn't need expanding
        DCHECK_EQ(_base->size(), _base->capacity());
        DCHECK(_base->capacity() < new_base_capacity);
        DLOG(INFO) << Format("%s memory=%.1lfM expand base base=%zu version=%zu",
                             _column->schema().to_string().c_str(),
                             _column->memory() / 1000000.0,
                             new_base_capacity,
                             new_version_capacity);
        RefPtr<Column> cow(new Column(*_column, new_base_capacity, new_version_capacity), false);
        cow.swap(_column);
        _base = &(_column->_base);
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
        if (_column->schema().cid == 1) {
            // only log when first column add page
            DLOG(INFO) << Format("Column(cid=%u) add ColumnPage %zu/%zu", _column->schema().cid, _base->size(), _base->capacity());
        }
        return Status::OK();
    }

    Status expand_delta() {
        size_t new_capacity = Padding(_column->_versions.capacity() + 64, 4);
        DLOG(INFO) << Format("%s memory=%.1lfM expand delta base=%zu version=%zu",
                             _column->schema().to_string().c_str(),
                             _column->memory() / 1000000.0,
                             _base->capacity(),
                             new_capacity);
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
    vector<ColumnDelta*> _deltas;

    size_t _num_insert = 0;
    size_t _num_update = 0;
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
    DLOG(INFO) << Format("create %s", to_string().c_str());
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

size_t Column::memory() const {
    size_t bs = _base.size();
    size_t ds = _versions.size();
    size_t base_memory = 0;
    for (size_t i=0;i<bs;i++) {
        base_memory += _base[i]->memory();
    }
    size_t delta_memory = 0;
    for (size_t i=0;i<ds;i++) {
        if (_versions[i].delta) {
            delta_memory += _versions[i].delta->memory();
        }
    }
    return base_memory + delta_memory;
}

string Column::to_string() const {
    string storage_info;
    if (_storage_type != _cs.type) {
        storage_info = Format(" storage:%s", TypeInfo::get(_cs.type).name().c_str());
    }
    return Format("Column(%s cid=%u version=%zu %s%s%s)",
            _cs.name.c_str(),
            _cs.cid,
            _versions.back().version,
            TypeInfo::get(_cs.type).name().c_str(),
            _cs.nullable?" null":"",
            storage_info.c_str());
}


Status Column::capture_version(uint64_t version, vector<ColumnDelta*>& deltas, uint64_t& real_version) const {
    uint64_t base_version = _versions[_base_idx].version;
    real_version = base_version;
    if (version < base_version) {
        uint64_t oldest = _versions[0].version;
        if (version < oldest) {
            return Status::NotFound(Format("version %zu(oldest=%zu) deleted", version, oldest));
        }
        DCHECK_GT(_base_idx, 0);
        for (ssize_t i = _base_idx-1; i>=0;i--) {
            uint64_t v = _versions[i].version;
            if (v >= version) {
                DCHECK(_versions[i].delta);
                real_version = v;
                deltas.emplace_back(_versions[i].delta.get());
                if (v == version) {
                    break;
                }
            } else {
                break;
            }
        }
    } else if (version > base_version) {
        size_t vsize = _versions.size();
        for (size_t i = _base_idx + 1; i < vsize; i++) {
            uint64_t v = _versions[i].version;
            if (v <= version) {
                DCHECK(_versions[i].delta);
                real_version = v;
                deltas.emplace_back(_versions[i].delta.get());
                if (v == version) {
                    break;
                }
            } else {
                break;
            }
        }
    }
    return Status::OK();
}

void Column::capture_latest(vector<ColumnDelta*>& deltas) const {
    deltas.reserve(_versions.size() - _base_idx - 1);
    for (size_t i=_base_idx+1;i<_versions.size();i++) {
        deltas.emplace_back(_versions[i].delta.get());
    }
}


Status Column::read(uint64_t version, unique_ptr<ColumnReader>& cr) {
    Type type = schema().type;
    bool nullable = schema().nullable;
    vector<ColumnDelta*> deltas;
    uint64_t real_version;
    RETURN_NOT_OK(capture_version(version, deltas, real_version));
    RefPtr<Column> pcol(this);
    switch (type) {
    case Int8:
        if (nullable) {
            cr.reset(new TypedColumnReader<int8_t, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<int8_t, false>(pcol, version, real_version, deltas));
        }
        break;
    case Int16:
        if (nullable) {
            cr.reset(new TypedColumnReader<int16_t, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<int16_t, false>(pcol, version, real_version, deltas));
        }
        break;
    case Int32:
        if (nullable) {
            cr.reset(new TypedColumnReader<int32_t, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<int32_t, false>(pcol, version, real_version, deltas));
        }
        break;
    case Int64:
        if (nullable) {
            cr.reset(new TypedColumnReader<int64_t, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<int64_t, false>(pcol, version, real_version, deltas));
        }
        break;
    case Int128:
        if (nullable) {
            cr.reset(new TypedColumnReader<int128_t, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<int128_t, false>(pcol, version, real_version, deltas));
        }
        break;
    case Float32:
        if (nullable) {
            cr.reset(new TypedColumnReader<float, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<float, false>(pcol, version, real_version, deltas));
        }
        break;
    case Float64:
        if (nullable) {
            cr.reset(new TypedColumnReader<double, true>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<double, false>(pcol, version, real_version, deltas));
        }
        break;
    case String:
        if (nullable) {
            cr.reset(new TypedColumnReader<Slice, true, int32_t>(pcol, version, real_version, deltas));
        } else {
            cr.reset(new TypedColumnReader<Slice, false, int32_t>(pcol, version, real_version, deltas));
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
