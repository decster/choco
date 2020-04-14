#ifndef CHOCO_WRITE_TX_H_
#define CHOCO_WRITE_TX_H_

#include "common.h"
#include "partial_row_batch.h"

namespace choco {

class WriteTx {
public:
    WriteTx(const Schema& schema);
    ~WriteTx();

    void add_batch(unique_ptr<PartialRowBatch>& batch);

    size_t batch_size() const { return _batches.size(); }

    const PartialRowBatch * get_batch(size_t idx) const;

private:
    Schema _schama;
    vector<unique_ptr<PartialRowBatch>> _batches;
};

} /* namespace choco */

#endif /* CHOCO_WRITE_TX_H_ */
