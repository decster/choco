#ifndef SRC_CHOCO_H_
#define SRC_CHOCO_H_

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <mutex>
#include <memory>
#include <algorithm>
#include "status.h"
#include "slice.h"

using std::string;
using std::vector;
using std::unordered_map;
using std::mutex;
using std::unique_ptr;

#define FATAL_EXIT exit(1);



enum Type {
    Nothing = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    Int128 = 5,
    Float32 = 6,
    Float64 = 7,
    String = 8
};

class TypeInfo {
public:
    static const TypeInfo& get(Type type);

    typedef void (*PrintDebugFunc)(const void*, string *);

    TypeInfo(Type type, const char* name, size_t size, PrintDebugFunc print_func);
    Type type() const { return _type; }
    const string& name() const { return _name; }
    const size_t size() const { return _size; }
    void print(const void* data, string* out) const {
        _print_func(data, out);
    }

private:
    Type _type;
    string _name;
    size_t _size;
    const PrintDebugFunc _print_func;
};


class alignas(16) Variant {
public:
    Variant(Type type, const void* value);
    Variant(const string& str);
    Variant(const char *data, size_t size);
    Variant(const Variant& rhs) : Variant(rhs.type(), rhs.value()) { }
    ~Variant() { clear(); }

    Type type() const { return _type; }

    const void* value() const { return &_value; }

    bool operator==(const Variant& rhs) const;

private:
    void reset(Type type, const void* value);

    void operator=(const Variant&) = delete;

    void clear();

    union alignas(16) Value {
        __int128 i128;
        int8_t i8;
        int16_t i16;
        int32_t i32;
        int64_t i64;
        float f32;
        double f64;
    };
    Value _value;
    Type _type;
};


class RefCounted {
public:
    RefCounted() : _ref_count(1) {}

    size_t addRef() {
        return ++_ref_count;
    }
    size_t decRef() {
        return --_ref_count;
    }
private:
    std::atomic<size_t> _ref_count;
};


template <class T>
class RefPtr {
public:
    RefPtr(T* obj=nullptr) : _obj(obj) {};

    RefPtr(const RefPtr& rhs) : _obj(rhs._obj) {
        addRef();
    }

    RefPtr(RefPtr&& rhs) : _obj(rhs._obj) {
        rhs._obj = nullptr;
    }

    ~RefPtr() {
        decRef();
    }

    void swap(RefPtr& rhs) {
        std::swap(_obj, rhs._obj);
    }

    RefPtr& operator=(const RefPtr& rhs) {
        RefPtr tmp = rhs;
        tmp.swap(tmp);
        return *this;
    }

    T* get() const {
        return _obj;
    }

    T& operator*() const {
        return *_obj;
    }

    operator bool() const {
        return _obj != nullptr;
    }

    void reset() {
        decRef();
    }

private:
    void addRef() {
        if (_obj) {
            _obj->addRef();
        }
    }

    void decRef() {
        if (_obj) {
            if (_obj->decRef() == 0) {
                delete _obj;
            }
            _obj = nullptr;
        }
    }

    T* _obj;
};


struct ColumnSchema {
    string name;
    uint32_t cid;
    Type type;
    bool nullable;
    unique_ptr<Variant> default_value;

    ColumnSchema(const ColumnSchema& rhs) {
        name = rhs.name;
        cid = rhs.cid;
        type = rhs.type;
        nullable = rhs.nullable;
        default_value.reset(new Variant(*rhs.default_value));
    }
};


class Schema {
public:
    Schema(vector<ColumnSchema>& columns, uint32_t num_key_column);
    ~Schema() = default;

    uint32_t next_cid() const;
    const ColumnSchema* get(const string& name) const;
    const ColumnSchema* get(uint32_t cid) const;

private:
    vector<ColumnSchema> _columns;
    uint32_t _num_key_column = 0;
    uint32_t _next_cid;
    unordered_map<string, ColumnSchema*> _name_to_col;
    unordered_map<uint32_t, ColumnSchema*> _cid_to_col;
};


class HashIndex {

};


class ColumnPage : public RefCounted {
};


class DeltaIndex : public RefCounted {

};


class ColumnDelta : public RefCounted {

};


class Column : public RefCounted {
public:
    Column(const ColumnSchema& cs);
private:
    ColumnSchema _schema;
    mutex _lock;
    uint32_t _base_idx = 0;
    vector<RefPtr<ColumnPage>> _base;
    struct VersionInfo {
        uint64_t version;
        // null if it's base
        RefPtr<ColumnDelta> delta;
    };
    vector<VersionInfo> _versions;
};


class MemSubTablet {
public:
    MemSubTablet() = default;

private:
    mutex _lock;
    HashIndex _index;
    struct VersionInfo {
        uint64_t version;
        uint64_t size;
    };
    vector<VersionInfo> _versions;
    unordered_map<uint32_t, RefPtr<Column>> _columns;
};


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


class MemTabletGet {
public:
    MemTabletGet(uint64_t version, const vector<string>& columns);

    void add_row();

private:
    uint64_t _version;
};

class MemTabletWrite {
public:
    MemTabletWrite();
private:

};


class MemTablet {
public:
    static Status load(const string& dir, uint64_t last_version, unique_ptr<MemTablet>& tablet);
    static Status create(const string& dir, unique_ptr<Schema>& schema, unique_ptr<MemTablet>& tablet);

    ~MemTablet() = default;

    Status scan(unique_ptr<ScanSpec>& spec, unique_ptr<MemTabletScan>& scan);
    Status get(unique_ptr<MemTabletGet>& get);
    Status create_write(unique_ptr<MemTabletWrite>& write);
    Status prepare_write(unique_ptr<MemTabletWrite>& write);
    Status commit_write(unique_ptr<MemTabletWrite>& write, uint64_t version);

private:
    MemTablet() = default;

    mutex _lock;
    struct VersionInfo {
        uint64_t version;
        unique_ptr<Schema> schema;
    };
    vector<VersionInfo> _versions;
    // TODO: support multiple subtablet in future
    unique_ptr<MemSubTablet> _sub_tablet;
};




#endif /* SRC_CHOCO_H_ */
