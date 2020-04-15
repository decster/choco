#include "write_tx.h"

namespace choco {

WriteTx::WriteTx(const Schema& schema) : _schema(schema) {
}

WriteTx::~WriteTx() {
}

PartialRowBatch* WriteTx::new_batch() {
    _batches.emplace_back(new PartialRowBatch(_schema, 1000000, 1<<15));
    return _batches.back().get();
}

const PartialRowBatch * WriteTx::get_batch(size_t idx) const {
    return _batches[idx].get();
}

} /* namespace choco */
