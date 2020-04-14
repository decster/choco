#include "write_tx.h"

namespace choco {

WriteTx::WriteTx(const Schema& schema) : _schama(schema) {
}

WriteTx::~WriteTx() {
}

void WriteTx::add_batch(unique_ptr<PartialRowBatch>& batch) {

}

const PartialRowBatch * WriteTx::get_batch(size_t idx) const {
    return _batches[idx].get();
}

} /* namespace choco */
