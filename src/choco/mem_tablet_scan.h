#ifndef CHOCO_MEM_TABLET_SCAN_H_
#define CHOCO_MEM_TABLET_SCAN_H_

#include "common.h"
#include "type.h"
#include "row_block.h"

namespace choco {

enum PredicateOp {
    OpEQ = 0x1,
    OpGT = 0x10,
    OpGE = 0x20,
    OpLT = 0x40,
    OpLE = 0x80,
    OpInList = 0x100,
};

class ColumnPredicate {
public:
    ColumnPredicate(PredicateOp uop, unique_ptr<Variant>& op) : op(uop) {
        op0.swap(op);
    }
    ColumnPredicate(PredicateOp bop, unique_ptr<Variant>& op0, unique_ptr<Variant>& op1) : op(bop) {
        op0.swap(op0);
        op1.swap(op1);
    }

    PredicateOp op;
    unique_ptr<Variant> op0;
    unique_ptr<Variant> op1;
};


class ColumnScan {
public:
    string name;
    bool proj = true;
    // currently only support AND
    vector<unique_ptr<ColumnPredicate>> predicates;
};


class ScanSpec {
public:
    static Status create(uint64_t version, const string &specstr,
            bool support_get, unique_ptr<ScanSpec> &spec);

    ScanSpec(uint64_t version, uint64_t limit,
            vector<unique_ptr<ColumnScan>> &cols, bool suport_get = false) :
            _version(version), _limit(limit), _support_get(suport_get) {
        _columns.swap(cols);
    }

    uint64_t version() const {
        return _version;
    }

    uint64_t limit() const {
        return _limit;
    }

    const vector<unique_ptr<ColumnScan>>& columns() const {
        return _columns;
    }

    bool support_get() const {
        return _support_get;
    }

private:
    uint64_t _version;
    uint64_t _limit;
    vector<unique_ptr<ColumnScan>> _columns;
    bool _support_get;
};

class HashIndex;
class MemTablet;
class ColumnReader;

class MemTabletScan {
public:
    ~MemTabletScan();

    struct GetResult {
        vector<int32_t> offsets;
        const RowBlock* block = nullptr;
    };

    /**
     * keys must be valid until all blocks are returned
     */
    Status get(GetResult& result, size_t nkey, const void * keys);
    Status get(GetResult& result, size_t nkey, const void * key0s, const void * key1s);

    /**
     * block content valid until next call to next_block
     */
    Status next_scan_block(const RowBlock*& block);

private:
    DISALLOW_COPY_AND_ASSIGN(MemTabletScan);
    MemTabletScan() = default;

    friend class MemTablet;

    Status setup();
    void setup_full_scan();
    Status setup_get_by_rids(vector<uint32_t>& rids);

    unique_ptr<ScanSpec> _spec;
    shared_ptr<MemTablet> _tablet;
    const Schema* _schema = nullptr;

    size_t _num_rows = 0;
    size_t _num_blocks = 0;
    // full scan support
    vector<unique_ptr<ColumnReader>> _readers;
    // get by row_key support
    vector<unique_ptr<ColumnReader>> _key_readers;
    RefPtr<HashIndex> _read_index;

    // returned block
    unique_ptr<RowBlock> _row_block;
    size_t _next_block = 0;
};


} /* namespace choco */

#endif /* CHOCO_MEM_TABLET_SCAN_H_ */
