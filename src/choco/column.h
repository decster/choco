#ifndef CHOCO_COLUMN_H_
#define CHOCO_COLUMN_H_

#include "common.h"
#include "schema.h"
#include "column_delta.h"

namespace choco {

class ColumnPage : public RefCounted {
public:
    ColumnPage() = default;

    void data();

private:
    uint64_t _pid = 0;
    Buffer   _nulls;
    Buffer   _data;
};


class ROColumn;


class Column : public RefCounted {
public:
    Column(const ColumnSchema& cs, uint64_t version);

    const ColumnSchema& schema() { return _cs; }

    Status read_at(uint64_t version, unique_ptr<ROColumn>& rc);

protected:
    friend class ROColumn;

    ColumnSchema _cs;
    mutex _lock;
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
 * Read only column, captures a specific version of Column
 */
class ROColumn {
public:
    virtual ~ROColumn() {}
    virtual Status get(uint32_t rid, void * dest) = 0;
    virtual uint64_t hashcode(uint32_t rid) = 0;
    virtual bool equals(uint32_t rid, void * rhs) = 0;

private:
    friend class Column;

    enum DeltaMode {
        NoDelta,
        Redo,
        Undo
    };

    ROColumn(RefPtr<Column>&& column, uint64_t real_version, vector<RefPtr<ColumnPage>>& base, DeltaMode mode, vector<ColumnDelta*>&& deltas);

    RefPtr<Column> _column;
    uint64_t _real_version;
    vector<RefPtr<ColumnPage>>& _base;
    DeltaMode _mode;
    vector<ColumnDelta*> _deltas;
};


} /* namespace choco */

#endif /* CHOCO_COLUMN_H_ */
