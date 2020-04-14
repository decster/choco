#ifndef CHOCO_MEM_TABLET_WRITER_H_
#define CHOCO_MEM_TABLET_WRITER_H_

#include "common.h"
#include "schema.h"
#include "write_tx.h"

namespace choco {

class MemTablet;
class Column;
class ColumnWriter;

class MemTabletWriter {
public:
    ~MemTabletWriter();

    const Schema& write_schema() const { return _write_schema; }

    Status init();

    Status set(unique_ptr<WriteTx>& tx);

    Status commit(uint64_t version);

private:
    friend class MemTablet;
    DISALLOW_COPY_AND_ASSIGN(MemTabletWriter);
    struct ColumnWriteState {
        RefPtr<Column> column;
        unique_ptr<ColumnWriter> writer;
    };

    MemTabletWriter(shared_ptr<MemTablet>&& tablet, const Schema& write_schema);

    ColumnWriteState& get_write(uint32_t cid);

    Status apply();

    shared_ptr<MemTablet> _tablet;
    Schema _write_schema;
    unique_ptr<WriteTx> _wtx;
    size_t num_rows;
    unordered_map<uint32_t, ColumnWriteState> _columns;
};

} /* namespace choco */

#endif /* CHOCO_MEM_TABLET_WRITER_H_ */
