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

    Status alloc(size_t size, size_t esize, BufferTag tag);

    bool is_null(uint32_t idx) {
        return _nulls && _nulls.as<bool>()[idx];
    }

    Status set_null(uint32_t idx);

    Status set_not_null(uint32_t idx);

private:
    uint64_t _pid = 0;
    size_t   _size = 0;
    BufferTag _tag = 0;
    Buffer   _nulls;
    Buffer   _data;
};


class ColumnReader;
class ColumnWriter;
template<class, bool, class> class TypedColumnReader;
template<class, bool, class, class> class TypedColumnWriter;


class Column : public RefCounted {
public:
    static const uint32_t BLOCK_SIZE = 1<<16;
    static const uint32_t BLOCK_MASK = 0xffff;

    Column(const ColumnSchema& cs, Type storage_type, uint64_t version);
    Column(const Column& rhs, size_t new_base_capacity, size_t new_version_capacity);

    const ColumnSchema& schema() { return _cs; }

    Status read(uint64_t version, unique_ptr<ColumnReader>& cr);

    Status write(unique_ptr<ColumnWriter>& cw);

    Status delta_compaction(RefPtr<Column>& result, uint64_t to_version);

private:
    DISALLOW_COPY_AND_ASSIGN(Column);

    template<class, bool, class> friend class TypedColumnReader;
    template<class, bool, class, class> friend class TypedColumnWriter;

    mutex _lock;
    ColumnSchema _cs;
    Type _storage_type;
    uint32_t _base_idx;
    vector<RefPtr<ColumnPage>> _base;
    struct VersionInfo {
        VersionInfo() = default;
        VersionInfo(uint64_t version) : version(version) {}
        uint64_t version = 0;
        // null if it's base
        RefPtr<ColumnDelta> delta;
    };
    vector<VersionInfo> _versions;
};


/**
 * Read only column reader, captures a specific version of a Column
 */
class ColumnReader {
public:
    virtual ~ColumnReader() {}
    /**
     * store value in *dest
     * return true if cell is not null, false if cell is null
     */
    virtual bool get(const uint32_t rid, void * dest) const = 0;
    /**
     * return hashcode of the cell
     */
    virtual uint64_t hashcode(const uint32_t rid) const = 0;
    /**
     * check cell value equals rhs
     */
    virtual bool equals(const uint32_t rid, void * rhs) const = 0;

protected:
    ColumnReader() = default;
};

/**
 * Exclusive writer for column, caller should keep a ref for Column object
 */
class ColumnWriter {
public:
    virtual ~ColumnWriter() {}
    virtual Status insert(uint32_t rid, const void * value) = 0;
    virtual Status update(uint32_t rid, const void * value) = 0;
    virtual Status finalize(uint64_t version, RefPtr<Column>& ret) = 0;

protected:
    ColumnWriter() = default;
};


} /* namespace choco */

#endif /* CHOCO_COLUMN_H_ */
