#ifndef CHOCO_TYPE_H_
#define CHOCO_TYPE_H_

#include "common.h"

namespace choco {

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


} /* namespace choco */

#endif /* CHOCO_TYPE_H_ */
