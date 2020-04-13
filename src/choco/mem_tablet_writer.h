#ifndef CHOCO_MEM_TABLET_WRITER_H_
#define CHOCO_MEM_TABLET_WRITER_H_

#include "common.h"
#include "schema.h"

namespace choco {

class MemTablet;


class MemTabletWriter {
public:
    ~MemTabletWriter();

    const Schema& write_schema() const { return _write_schema; }

    Status init();

    // for test with demo table (int32 id,int32 uv,int32 pv,int8 city null)
    Status insert(int32_t key, int32_t uv, int32_t pv, int8_t * pcity);

    Status update_uv(int32_t key, int32_t uv);

    Status update_pv(int32_t key, int32_t pv);

    Status update_city(int32_t key, int8_t* pcity);

    Status commit(uint64_t version);

private:
    friend class MemTablet;
    DISALLOW_COPY_AND_ASSIGN(MemTabletWriter);

    MemTabletWriter(shared_ptr<MemTablet>&& tablet, const Schema& write_schema);

    shared_ptr<MemTablet> _tablet;
    Schema _write_schema;
};

} /* namespace choco */

#endif /* CHOCO_MEM_TABLET_WRITER_H_ */
