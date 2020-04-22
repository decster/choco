#ifndef CHOCO_ROW_BLOCK_H_
#define CHOCO_ROW_BLOCK_H_

#include "common.h"
#include "schema.h"
#include "buffer.h"

namespace choco {

class ColumnBlock {
public:
    ColumnBlock() = default;
    ~ColumnBlock();
    void clear();
    Status alloc(size_t size, size_t esize);
    Status copy_from(size_t size, size_t esize, Buffer& data, Buffer& nulls);

    const uint8_t* data() const {
        return _data;
    }
    const uint8_t* nulls() const {
        return _nulls;
    }

private:
    friend class RowBlock;
    template <class, bool, class> friend class TypedColumnReader;

    const ColumnSchema* _cs = nullptr;
    uint8_t* _data = nullptr;
    uint8_t* _nulls = nullptr;
    size_t _owned_size = 0;
};

class RowBlock {
public:
    size_t num_rows() const {
        return _nrows;
    }
    size_t num_columns() const {
        return _columns.size();
    }
    const ColumnBlock& get_column(size_t idx) const {
        return _columns[idx];
    }

private:
    friend class MemTabletScan;
    RowBlock() = default;

    size_t _nrows = 0;
    vector<ColumnBlock> _columns;
};

} /* namespace choco */

#endif /* CHOCO_ROW_BLOCK_H_ */
