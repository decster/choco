#include "mem_tablet_scan.h"
#include "column.h"
#include "mem_tablet.h"
#include "mem_sub_tablet.h"

namespace choco {

Status ScanSpec::create(uint64_t version, const string &specstr,
        bool support_get, unique_ptr<ScanSpec> &spec) {
    vector<Slice> colss = Slice(specstr).split(',', true);
    if (colss.size() == 0) {
        return Status::InvalidArgument("invalid scan spec string");
    }
    vector<unique_ptr<ColumnScan>> colscans;
    for (auto& cols : colss) {
        unique_ptr<ColumnScan> pcs(new ColumnScan());
        pcs->name = cols.ToString();
        pcs->proj = true;
        colscans.emplace_back(std::move(pcs));
    }
    spec.reset(new ScanSpec(version, -1, colscans, support_get));
    return Status::OK();
}


MemTabletScan::~MemTabletScan() {
}

Status MemTabletScan::setup() {
    // setup num_rows
    RETURN_NOT_OK(_tablet->_sub_tablet->get_size(_spec->version(), _num_rows));
    // setup full scan readers
    auto& columns = _spec->columns();
    _readers.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        const ColumnSchema* cs = _schema->get(columns[i]->name);
        if (!cs) {
            return Status::NotFound("column not found for scan");
        }
        RETURN_NOT_OK(_tablet->_sub_tablet->read_column(_spec->version(), cs->cid, _readers[i]));
    }
    // setup read by row_key
    if (_spec->support_get()) {
        _key_readers.resize(_schema->num_key_column());
        for (size_t i = 0; i < _schema->num_key_column(); ++i) {
            RETURN_NOT_OK(_tablet->_sub_tablet->read_column(_spec->version(), i+1, _key_readers[i]));
        }
        RETURN_NOT_OK(_tablet->_sub_tablet->read_index(_read_index));
    }
    // setup row block for full scan by default
    _row_block.reset(new RowBlock());
    _row_block->_columns.resize(columns.size());
    setup_full_scan();
    return Status::OK();
}

void MemTabletScan::setup_full_scan() {
    _next_block = 0;
    _num_blocks = NBlock(_num_rows, Column::BLOCK_SIZE);
}


Status MemTabletScan::get(GetResult& result, size_t nkey, const void * keys) {
    result.offsets.resize(nkey);
    size_t next_offset = 0;
    vector<uint32_t> rids;
    rids.reserve(nkey);

    std::vector<HashIndex::Entry> entries;
    entries.reserve(8);
    for (size_t i=0;i<nkey;i++) {
        uint64_t keyhash = _key_readers[0]->hashcode(keys, i);
        entries.clear();
        _read_index->find(keyhash, entries);
        bool found = false;
        for (auto& e : entries) {
            uint32_t rid = e.value;
            if (rid >= _num_rows) {
                // future rows
                continue;
            }
            if (_key_readers[0]->equals(rid, keys, i)) {
                rids.emplace_back(rid);
                result.offsets[i] = next_offset++;
                found = true;
            }
        }
        if (!found) {
            result.offsets[i] = -1;
        }
    }
    return setup_get_by_rids(rids);
}

Status MemTabletScan::get(GetResult& result, size_t nkey, const void * key0s, const void * key1s) {
    return Status::NotSupported("get by multi-column row key not supported");
}

Status MemTabletScan::setup_get_by_rids(vector<uint32_t>& rids) {
    for (size_t i = 0; i < _readers.size(); ++i) {
        RETURN_NOT_OK(_readers[i]->get_by_rids(rids, _row_block->_columns[i]));
    }
    return Status::OK();
}

Status MemTabletScan::next_scan_block(const RowBlock*& block) {
    if (_next_block >= _num_blocks) {
        block = nullptr;
        return Status::OK();
    }
    size_t rows_in_block = std::min((size_t)Column::BLOCK_SIZE,
            _num_rows - _next_block*Column::BLOCK_SIZE);
    _row_block->_nrows = rows_in_block;
    for (size_t i = 0; i < _readers.size(); ++i) {
        RETURN_NOT_OK(_readers[i]->get_block(rows_in_block, _next_block, _row_block->_columns[i]));
    }
    _next_block++;
    block = _row_block.get();
    return Status::OK();
}

} /* namespace choco */
