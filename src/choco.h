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

using std::string;
using std::vector;
using std::unordered_map;
using std::mutex;
using std::unique_ptr;


class RefCounted {
public:
    RefCounted() = default;

    size_t addRef() {
        return ++_ref_count;
    }
    size_t decRef() {
        return --_ref_count;
    }
private:
    std::atomic<size_t> _ref_count = 1;
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



class HashIndex {

};


class ColumnArray : public RefCounted {
};


class DeltaIndex : public RefCounted {

};


class ColumnDelta : public RefCounted {

};


class Column : public RefCounted {
public:
    Column() = default;
private:
    mutex _lock;
    uint64_t _vstart = 0;
    uint64_t _vend = UINT64_MAX;
    uint32_t _cid = 0;
    uint32_t _base_idx = 0;
    vector<RefPtr<ColumnArray>> _base;
    struct DeltaInfo {
        uint64_t version;
        RefPtr<ColumnDelta> delta;
    };
    vector<DeltaInfo> _deltas;
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
    unordered_map<string, vector<RefPtr<Column>>> _columns;
};


class ColumnScan {
public:
    ColumnScan() = default;
private:
    string _name;
    bool _proj;

};

class ScanInfo {
public:
    ScanInfo() = default;
private:
    uint64_t _version;
    vector<ColumnScan> _columns;
};

class MemTabletScan;

class MemTablet {
public:
    MemTablet() = default;
    unique_ptr<MemTabletScan> scan(uint64_t )
private:
    vector<unique_ptr<MemSubTablet>> _sub_tablets;
};


class MemTabletScan {
public:
    MemTabletScan() = default;
private:

};



#endif /* SRC_CHOCO_H_ */
