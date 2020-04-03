#ifndef CHOCO_COLUMN_H_
#define CHOCO_COLUMN_H_

#include "common.h"
#include "schema.h"
#include "column_delta.h"

namespace choco {

class ColumnPage : public RefCounted {
public:
    ColumnPage() = default;

    Buffer& data() { return _data; }
    Buffer& nulls() { return _nulls; }

private:
    uint64_t _pid = 0;
    Buffer   _nulls;
    Buffer   _data;
};


class ROColumn;


class Column : public RefCounted {
public:
    const uint32_t BLOCK_SIZE = 1<<16;
    const uint32_t BLOCK_MASK = 0xffff;

    Column(const ColumnSchema& cs, uint64_t version);

    const ColumnSchema& schema() { return _cs; }

    Status read_at(uint64_t version, unique_ptr<ROColumn>& rc);

    size_t capacity() { return _capacity; }

    /**
     * return new capacity
     */
    Status reserve(size_t size);

    void set(uint32_t rid, void * value);

protected:
    friend class ROColumn;

    mutex _lock;
    ColumnSchema _cs;
    Type _storage_type;
    std::atomic<size_t> _capacity;
    uint32_t _base_idx = 0;
    vector<RefPtr<ColumnPage>> _base;
    struct VersionInfo {
        VersionInfo(uint64_t version) : version(version) {}
        uint64_t version;
        // null if it's base
        RefPtr<ColumnDelta> delta;
    };
    vector<VersionInfo> _versions;
};


/**
 * Read only column, captures a specific version of a Column
 */
class ROColumn {
public:
    virtual ~ROColumn() {}

    virtual bool get(const uint32_t rid, void * dest) const = 0;

    virtual uint64_t hashcode(const uint32_t rid) const = 0;

    virtual bool equals(const uint32_t rid, void * rhs) const = 0;

protected:
    friend class Column;

    ROColumn(RefPtr<Column>& column, uint64_t real_version, vector<ColumnPage*>& base, vector<ColumnDelta*>& deltas);

    RefPtr<Column> _column;
    uint64_t _real_version;
    vector<ColumnPage*>& _base;
    vector<ColumnDelta*> _deltas;
};


} /* namespace choco */

#endif /* CHOCO_COLUMN_H_ */
