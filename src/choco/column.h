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
template<class, bool, class, class> class TypedColumnWriter;

class Column : public RefCounted {
public:
    static const uint32_t BLOCK_SIZE = 1<<16;
    static const uint32_t BLOCK_MASK = 0xffff;

    Column(const ColumnSchema& cs, uint64_t version);

    const ColumnSchema& schema() { return _cs; }

    size_t capacity() { return _capacity; }

    Status read_at(uint64_t version, unique_ptr<ColumnReader>& cr);

    Status start_write(unique_ptr<ColumnWriter>& cw, bool wait=false);
    Status cancel_write(unique_ptr<ColumnWriter>& cw);
    Status finish_write(unique_ptr<ColumnWriter>& cw, uint64_t version);

    Status delta_compaction(RefPtr<Column>& result, uint64_t to_version);

private:
    friend class ColumnReader;
    friend class ColumnWriter;
    template<class, bool, class, class> friend class TypedColumnWriter;

    void prepare_writer();

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

    // write state
    mutex _wlock;
    unique_ptr<ColumnWriter> _writer;
};


/**
 * Read only column reader, captures a specific version of a Column
 */
class ColumnReader {
public:
    virtual ~ColumnReader() {}

    virtual bool get(const uint32_t rid, void * dest) const = 0;

    virtual uint64_t hashcode(const uint32_t rid) const = 0;

    virtual bool equals(const uint32_t rid, void * rhs) const = 0;

protected:
    friend class Column;

    ColumnReader(RefPtr<Column>& column, uint64_t real_version, vector<ColumnPage*>& base, vector<ColumnDelta*>& deltas);

    RefPtr<Column> _column;
    uint64_t _real_version;
    vector<ColumnPage*>& _base;
    vector<ColumnDelta*> _deltas;
};

/**
 * Exclusive writer for column, caller should keep a ref for Column object
 */
class ColumnWriter {
public:
    virtual ~ColumnWriter() {}
    virtual Status reserve(size_t size);
    virtual Status insert(uint32_t rid, const void * value);
    virtual Status update(uint32_t rid, const void * value);
    virtual Status finalize();

protected:
    friend class Column;

    ColumnWriter(Column* column);

    Column* _column;
    vector<RefPtr<ColumnPage>>& _base;
    RefPtr<ColumnDelta> _delta;
};


} /* namespace choco */

#endif /* CHOCO_COLUMN_H_ */
