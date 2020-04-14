#include "partial_row_batch.h"

#include "bitmap.h"

namespace choco {

PartialRowBatch::PartialRowBatch(const Schema& schema, size_t byte_capacity, size_t row_capacity) :
    _schema(schema) {
    _data = (uint8_t*)aligned_malloc(byte_capacity, 64);
    _bsize = 0;
    if (!_data) {
        _byte_capacity = 0;
        _row_capacity = 0;
    }
    _byte_capacity = byte_capacity;
    _row_capacity = row_capacity;
}

PartialRowBatch::~PartialRowBatch() {
    if (_data) {
        aligned_free(_data);
        _data = nullptr;
    }
    _bsize = 0;
    _byte_capacity = 0;
    _row_capacity = 0;
}


const uint8_t* PartialRowBatch::get_row(size_t idx) const {
    if (!_data || idx >= _row_offsets.size()) {
        return nullptr;
    }
    //DLOG(INFO) << Format("get_row %zu at %zu", idx, _row_offsets[idx] + 4);
    return _data + _row_offsets[idx] + 4;
}

//////////////////////////////////////////////////////////////////////////////

PartialRowWriter::PartialRowWriter(PartialRowBatch& batch) :
        _batch(batch),
        _schema(&batch.schema()),
        _bit_set_size(_schema->cid_size()),
        _bit_null_size(0) {
    _temp_cells.resize(_schema->cid_size());
}

PartialRowWriter::~PartialRowWriter() {

}

void PartialRowWriter::start_row() {
    _bit_null_size = 0;
    memset(&(_temp_cells[0]), 0, sizeof(CellInfo)*_temp_cells.size());
}

Status PartialRowWriter::end_row() {
    if (_batch._row_offsets.size() >= _batch.row_capacity()) {
        return Status::InvalidArgument("over capacity");
    }
    size_t row_byte_size = byte_size();
    if (_batch.byte_size() + row_byte_size + 4 > _batch.byte_capacity()) {
        return Status::InvalidArgument("over capacity");
    }
    *(uint32_t*)(_batch._data + _batch._bsize) = row_byte_size;
    uint8_t* pos = _batch._data + _batch._bsize + 4;
    RETURN_NOT_OK(write(pos));
    //DLOG(INFO) << Format("write row %zu at %zu", _batch._row_offsets.size(), _batch._bsize+4);
    _batch._row_offsets.push_back(_batch._bsize);
    _batch._bsize = pos - _batch._data;
    return Status::OK();
}


Status PartialRowWriter::set(const string& col, void* data) {
    auto cs =  _schema->get(col);
    if (!cs) {
        return Status::NotFound("col name not found");
    }
    if (cs->type == String && data) {
        if (((Slice*)data)->size() > 65535) {
            return Status::InvalidArgument("string length > 65535");
        }
    }
    uint32_t cid = cs->cid;
    if (cs->nullable || data) {
        if (cs->nullable && !_temp_cells[cid].isnullable) {
            _bit_null_size++;
        }
        _temp_cells[cid].isnullable = cs->nullable;
        _temp_cells[cid].isset = 1;
        _temp_cells[cid].data = (uint8_t*)data;
    } else {
        return Status::InvalidArgument("not nullable");
    }
    return Status::OK();
}

Status PartialRowWriter::set(uint32_t cid, void* data) {
    auto cs = _schema->get(cid);
    if (!cs) {
        return Status::NotFound("cid not found");
    }
    if (cs->type == String && data) {
        if (((Slice*)data)->size() > 65535) {
            return Status::InvalidArgument("string length > 65535");
        }
    }
    if (cs->nullable || data) {
        if (cs->nullable && !_temp_cells[cid].isnullable) {
            _bit_null_size++;
        }
        _temp_cells[cid].isnullable = cs->nullable;
        _temp_cells[cid].isset = 1;
        _temp_cells[cid].data = (uint8_t*)data;
    } else {
        return Status::InvalidArgument("not nullable");
    }
    return Status::OK();
}

Status PartialRowWriter::set_delete() {
    _temp_cells[0].isset = 1;
    return Status::OK();
}

size_t PartialRowWriter::byte_size() const {
    // TODO: support delete
    size_t bit_all_size = NBlock(_bit_set_size+_bit_null_size, 8);
    size_t data_size = 2 + bit_all_size;
    for (size_t i=1;i<_temp_cells.size();i++) {
        if (_temp_cells[i].data) {
            Type type = _schema->get(i)->type;
            if (type == String) {
                data_size += 2 + ((Slice*)(_temp_cells[i].data))->size();
            } else {
                data_size += TypeInfo::get(type).size();
            }
        }
    }
    return data_size;
}

Status PartialRowWriter::write(uint8_t*& pos) {
    size_t bit_all_size = NBlock(_bit_set_size+_bit_null_size, 8);
    if (bit_all_size >= 65536) {
        return Status::NotSupported("too many columns");
    }
    *(uint16_t*)pos = (uint16_t)bit_all_size;
    pos += 2;
    uint8_t* bitvec = pos;
    pos += bit_all_size;
    memset(bitvec, 0, bit_all_size);
    if (_temp_cells[0].isset) {
        // deleted
        BitmapSet(bitvec, 0);
    }
    size_t cur_nullable_idx = _bit_set_size;
    for (size_t i=1;i<_temp_cells.size();i++) {
        if (_temp_cells[i].isset) {
            BitmapSet(bitvec, i);
            if (_temp_cells[i].isnullable) {
                if (_temp_cells[i].data == nullptr) {
                    BitmapSet(bitvec, cur_nullable_idx);
                }
                cur_nullable_idx++;
            }
            uint8_t *pdata = _temp_cells[i].data;
            if (pdata) {
                Type type = _schema->get(i)->type;
                if (type == String) {
                    size_t sz = ((Slice*)pdata)->size();
                    *(uint16_t*)pos = (uint16_t)sz;
                    pos += 2;
                    memcpy(pos, ((Slice*)pdata)->data(), sz);
                    pos += sz;
                } else {
                    size_t sz = TypeInfo::get(type).size();
                    memcpy(pos, _temp_cells[i].data, sz);
                    pos += sz;
                }
            }
        } else if (i <= _schema->num_key_column()) {
            return Status::InvalidArgument("build without key columns");
        }
    }
    return Status::OK();
}



PartialRowReader::PartialRowReader(PartialRowBatch& batch) :
        _batch(batch),
        _schema(&batch.schema()),
        _bit_set_size(_schema->cid_size()),
        _delete(false) {
    _cells.reserve(_schema->columns().size());
}

Status PartialRowReader::read(size_t idx) {
    _delete = false;
    _cells.clear();
    const uint8_t * cur = _batch.get_row(idx);
    if (!cur) {
        return Status::InvalidArgument("idx out of range");
    }
    size_t bit_all_size = *(uint16_t*)cur;
    //DLOG(INFO) << Format("bit_all_size %zu pos: %p", bit_all_size, cur);
    cur += 2;
    DCHECK(bit_all_size <= 65535);
    const uint8_t * bitvec = cur;
    cur += bit_all_size;
    size_t cur_nullable_idx = _bit_set_size;
    if (BitmapTest(bitvec, 0)) {
        _delete = true;
    }
    size_t offset = 1;
    while (BitmapFindFirstSet(bitvec, offset, _bit_set_size, &offset)) {
        //DLOG(INFO) << "get bitset " << offset;
        const ColumnSchema* cs = _schema->get(offset);
        DCHECK(cs);
        if (cs->nullable) {
            if (BitmapTest(bitvec, cur_nullable_idx)) {
                // is null
                _cells.emplace_back(offset, nullptr);
            } else {
                // not null
                _cells.emplace_back(offset, cur);
            }
            cur_nullable_idx++;
        } else {
            _cells.emplace_back(offset, cur);
        }
        const uint8_t* pdata = _cells.back().data;
        if (pdata) {
            Type type = _schema->get(offset)->type;
            if (type == String) {
                size_t sz = *(uint16_t*)cur;
                cur += (sz + 2);
            } else {
                size_t sz = TypeInfo::get(type).size();
                cur += sz;
            }
        }
        offset++;
    }
    return Status::OK();
}


Status PartialRowReader::get_cell(size_t idx, const ColumnSchema*& cs, const void*& data) const {
    if (idx >= _cells.size()) {
        return Status::InvalidArgument("idx exceed size");
    }
    auto& cell = _cells[idx];
    cs = _schema->get(cell.cid);
    data = cell.data;
    return Status::OK();
}

} /* namespace choco */
