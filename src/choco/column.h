#ifndef CHOCO_COLUMN_H_
#define CHOCO_COLUMN_H_

#include "common.h"
#include "schema.h"
#include "column_delta.h"

namespace choco {

class ColumnPage : public RefCounted {
public:
    ColumnPage() = default;

    size_t memory() const;

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

    size_t memory() const;

    Status read(uint64_t version, unique_ptr<ColumnReader>& cr);

    Status write(unique_ptr<ColumnWriter>& cw);

    Status delta_compaction(RefPtr<Column>& result, uint64_t to_version);

    string to_string() const;

private:
    DISALLOW_COPY_AND_ASSIGN(Column);

    template<class, bool, class> friend class TypedColumnReader;
    template<class, bool, class, class> friend class TypedColumnWriter;

    Status capture_version(uint64_t version, vector<ColumnDelta*>& deltas, uint64_t& real_version) const;
    void capture_latest(vector<ColumnDelta*>& deltas) const;

    mutex _lock;
    ColumnSchema _cs;
    Type _storage_type;
    ssize_t _base_idx;
    // TODO: not strictly thread-safe, use a thread-safe append only vector instead
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
     * get cell by rid, caller need to make sure rid is in range
     * otherwise undefined behavior
     */
    virtual const void * get(const uint32_t rid) const = 0;

    /**
     * get basic info about this reader
     */
    virtual string to_string() const = 0;

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
    virtual Status finalize(uint64_t version) = 0;
    virtual Status get_new_column(RefPtr<Column>& ret) = 0;
    virtual string to_string() const = 0;

    virtual const void * get(const uint32_t rid) const = 0;
    // get then check equality for fast key lookup
    virtual bool equals(const uint32_t rid, const void * rhs) const = 0;
    // borrow a virtual function slot to do typed hash
    virtual uint64_t hashcode(const void * data) const = 0;

protected:
    ColumnWriter() = default;
};


} /* namespace choco */

#endif /* CHOCO_COLUMN_H_ */
