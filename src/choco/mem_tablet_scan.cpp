#include "mem_tablet_scan.h"
#include "column.h"
#include "mem_tablet.h"
#include "mem_sub_tablet.h"

namespace choco {

Status ScanSpec::create(uint64_t version, const string& specstr, unique_ptr<ScanSpec>& spec) {
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
    spec.reset(new ScanSpec(version, -1, colscans));
    return Status::OK();
}


MemTabletScan::~MemTabletScan() {
}

Status MemTabletScan::setup() {
    // setup num_rows
    RETURN_NOT_OK(_tablet->_sub_tablet->get_size(_spec->version(), _num_rows));
    // setup readers
    auto& columns = _spec->columns();
    _readers.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        const ColumnSchema* cs = _schema->get(columns[i]->name);
        if (!cs) {
            return Status::NotFound("column not found for scan");
        }
        RETURN_NOT_OK(_tablet->_sub_tablet->read_column(_spec->version(), cs->cid, _readers[i]));
    }
    // setup row block
    _row_block.reset(new RowBlock());
    _row_block->_columns.resize(columns.size());
    _next_block = 0;
    _num_blocks = NBlock(_num_rows, Column::BLOCK_SIZE);
    return Status::OK();
}

Status MemTabletScan::next_block(const RowBlock*& block) {
    if (_next_block >= _num_blocks) {
        block = nullptr;
        return Status::OK();
    }
    size_t rows_in_block = std::min((size_t)Column::BLOCK_SIZE,
            _num_rows - _next_block*Column::BLOCK_SIZE);
    _row_block->_nrows = rows_in_block;
    for (size_t i = 0; i < _readers.size(); ++i) {
        _readers[i]->get_block(rows_in_block, _next_block, _row_block->_columns[i]);
    }
    _next_block++;
    block = _row_block.get();
    return Status::OK();
}

} /* namespace choco */
