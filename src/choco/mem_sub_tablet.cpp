#include "mem_sub_tablet.h"
#include "partial_row_batch.h"

namespace choco {

Status MemSubTablet::create(uint64_t version, const Schema& schema, unique_ptr<MemSubTablet>& ret) {
	unique_ptr<MemSubTablet> tmp(new MemSubTablet());
	tmp->_versions.reserve(64);
	tmp->_versions.emplace_back(version, 0);
	tmp->_columns.resize(schema.cid_size());
	for (auto& c : schema.columns()) {
		// TODO: support storage_type != c.type
		RefPtr<Column> column(new Column(c, c.type, version), false);
		tmp->_columns[c.cid].swap(column);
	}
	tmp.swap(ret);
	return Status::OK();
}

MemSubTablet::MemSubTablet() :
	_index(new HashIndex(1<<16), false) {
}

MemSubTablet::~MemSubTablet() {
}

Status MemSubTablet::get_size(uint64_t version, size_t& size) const {
    std::lock_guard<mutex> lg(_lock);
    if (version == -1) {
        // get latest
        size = _versions.back().size;
        return Status::OK();
    }
    if (_versions[0].version > version) {
        return Status::NotFound("get_size failed, version too old");
    }
    for (size_t i=1;i<_versions.size();i++) {
        if (_versions[i].version > version) {
            size = _versions[i-1].size;
            return Status::OK();
        }
    }
    size = _versions.back().size;
    return Status::OK();
}

Status MemSubTablet::read_column(uint64_t version, uint32_t cid, unique_ptr<ColumnReader>& reader) {
    RefPtr<Column> cl;
    {
        std::lock_guard<mutex> lg(_lock);
        if (cid < _columns.size()) {
            cl = _columns[cid];
        }
    }
    if (!cl) {
        return Status::NotFound("column not found");
    }
    return cl->read(version, reader);
}

Status MemSubTablet::prepare_writer_for_column(uint32_t cid) {
    if (!_writers[cid]) {
        DLOG(INFO) << "create column writer for: " << cid;
        RETURN_NOT_OK(_columns[cid]->write(_writers[cid]));
    }
    return Status::OK();
}

Status MemSubTablet::begin_write(const Schema& schema) {
    _schema = &schema;
    _row_size = latest_size();
    _write_index = _index;
    _writers.clear();
    _writers.resize(_columns.size());
    // precache key columns
    for (size_t i=0;i<schema.num_key_column();i++) {
        prepare_writer_for_column(i+1);
    }
    _temp_hash_entries.reserve(8);
    return Status::OK();
}

Status MemSubTablet::apply_partial_row(const PartialRowReader& row) {
    DCHECK(row.cell_size() >= 1);
    const ColumnSchema* dsc;
    const void * key;
    // get key column
    row.get_cell(0, dsc, key);
    ColumnWriter* keyw = _writers[1].get();
    uint64_t hashcode = keyw->hashcode(key);
    _temp_hash_entries.clear();
    uint32_t newslot = _write_index->find(hashcode, _temp_hash_entries);
    uint32_t rid = -1;
    for (size_t i=0;i<_temp_hash_entries.size();i++) {
        uint32_t test_rid = _temp_hash_entries[i].value;
        if (keyw->equals(test_rid, key)) {
            rid = test_rid;
            break;
        }
    }
    if (rid == -1) {
        // insert
        rid = _row_size;
        // add all columns
        //DLOG(INFO) << Format("insert rid=%u", rid);
        for (size_t i=0;i<row.cell_size();i++) {
            const void * data;
            RETURN_NOT_OK(row.get_cell(i, dsc, data));
            uint32_t cid = dsc->cid;
            RETURN_NOT_OK(prepare_writer_for_column(cid));
            RETURN_NOT_OK(_writers[cid]->insert(rid, data));
        }
        _write_index->set(newslot, hashcode, rid);
        _row_size++;
    } else {
        // add non-key columns
        for (size_t i=1;i<row.cell_size();i++) {
            const void * data;
            RETURN_NOT_OK(row.get_cell(i, dsc, data));
            uint32_t cid = dsc->cid;
            if (cid > _schema->num_key_column()) {
                RETURN_NOT_OK(prepare_writer_for_column(cid));
                RETURN_NOT_OK(_writers[cid]->update(rid, data));
            }
        }
    }
    if (_write_index->need_rehash()) {
        RefPtr<HashIndex> new_index;
        // TODO: trace and limit memory usage
        size_t new_capacity = _row_size * 2;
        while (true) {
            new_index = rebuild_hash_index(new_capacity);
            if (new_index) {
                break;
            } else {
                new_capacity += 1<<16;
            }
        }
        _write_index = new_index;
    }
    return Status::OK();
}

Status MemSubTablet::commit_write(uint64_t version) {
    for (size_t cid=0;cid<_writers.size();cid++) {
        if (_writers[cid]) {
            _writers[cid]->finalize(version);
        }
    }
    {
        std::lock_guard<mutex> lg(_lock);
        if (_index != _write_index) {
            _index = _write_index;
        }
        for (size_t cid=0;cid<_writers.size();cid++) {
            if (_writers[cid]) {
                _writers[cid]->get_new_column(_columns[cid]);
            }
        }
        _versions.emplace_back(version, _row_size);
    }
    _writers.clear();
    return Status::OK();
}

RefPtr<HashIndex> MemSubTablet::rebuild_hash_index(size_t new_capacity) {
    double t0 = Time();
    ColumnWriter* keyw = _writers[1].get();
    RefPtr<HashIndex> hi(new HashIndex(new_capacity), false);
    for (size_t i=0;i<_row_size;i++) {
        const void * data = keyw->get(i);
        DCHECK_NOTNULL(data);
        uint64_t hashcode = keyw->hashcode(data);
        if (!hi->add(hashcode, i)) {
            double t1 = Time();
            LOG(INFO) << Format("Rebuild hash index %zu failed time: %.3lfs, expand", new_capacity, t1-t0);
            return RefPtr<HashIndex>();
        }
    }
    double t1 = Time();
    LOG(INFO) << Format("Rebuild hash index %zu time: %.3lfs", new_capacity, t1-t0);
    return hi;
}


} /* namespace choco */
