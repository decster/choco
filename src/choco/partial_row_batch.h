#ifndef CHOCO_PARTIAL_ROW_BATCH_H_
#define CHOCO_PARTIAL_ROW_BATCH_H_

#include <map>
#include "common.h"
#include "schema.h"

namespace choco {


class PartialRowBatch {
public:
    PartialRowBatch(const Schema& schema, size_t byte_capacity, size_t row_capacity);
    ~PartialRowBatch();

    const Schema& schema() const {return _schema;}

    size_t row_size() const { return _row_offsets.size(); }
    size_t row_capacity() const { return _row_capacity; }
    size_t byte_size() const { return _bsize; }
    size_t byte_capacity() const { return _byte_capacity; }

    const uint8_t * get_row(size_t idx) const;

private:
    friend class PartialRowWriter;
    friend class PartialRowReader;
    Schema _schema;
    vector<uint32_t> _row_offsets;
    uint8_t* _data;
    size_t   _bsize;
    size_t   _byte_capacity;
    size_t   _row_capacity;
};


class PartialRowWriter {
public:
    PartialRowWriter(const Schema& schema);
    ~PartialRowWriter();

    void start_row();
    Status write_row_to_batch(PartialRowBatch& batch);

    /**
     * set cell value by column name
     * @param data's memory must be valid before calling build
     */
    Status set(const string& col, void* data);
    Status set(uint32_t cid, void* data);
    Status set_delete();

private:
    size_t byte_size() const;
    Status write(uint8_t*& pos);

    const Schema& _schema;
    struct CellInfo {
        CellInfo() = default;
        uint32_t isset = 0;
        uint32_t isnullable = 0;
        uint8_t* data = nullptr;
    };
    vector<CellInfo> _temp_cells;
    size_t _bit_set_size;
    size_t _bit_null_size;
};


class PartialRowReader {
public:
    PartialRowReader(const PartialRowBatch& batch);
    size_t size() const { return _batch.row_size(); }
    Status read(size_t idx);
    size_t cell_size() const { return _cells.size(); }
    Status get_cell(size_t idx, const ColumnSchema*& cs, const void*& data) const;

private:
    const PartialRowBatch& _batch;
    const Schema* _schema;
    bool _delete;
    size_t _bit_set_size;
    struct CellInfo {
        CellInfo(uint32_t cid, const void * data) : cid(cid), data((uint8_t*)data) {}
        uint32_t cid = 0;
        const uint8_t* data = nullptr;
    };
    vector<CellInfo> _cells;
};


} /* namespace choco */

#endif /* CHOCO_PARTIAL_ROW_BATCH_H_ */
