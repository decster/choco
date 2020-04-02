#ifndef CHOCO_MEM_TABLET_SCAN_H_
#define CHOCO_MEM_TABLET_SCAN_H_

#include "common.h"
#include "type.h"

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

private:
    PredicateOp op;
    unique_ptr<Variant> op0;
    unique_ptr<Variant> op1;
};


class ColumnScan {
public:
    ColumnScan() = default;
private:
    string _name;
    bool _proj = true;
    // currently only support AND
    vector<unique_ptr<ColumnPredicate>> predicates;
};


class ScanSpec {
public:
    ScanSpec(uint64_t version, uint64_t limit, vector<unique_ptr<ColumnScan>>& cols) : _version(version), _limit(limit) {
        _columns.swap(cols);
    }

private:
    uint64_t _version;
    uint64_t _limit;
    vector<unique_ptr<ColumnScan>> _columns;
};


class MemTabletScan {
public:
    MemTabletScan();

private:
    unique_ptr<ScanSpec> _spec;
    // hold references
};


} /* namespace choco */

#endif /* CHOCO_MEM_TABLET_SCAN_H_ */
