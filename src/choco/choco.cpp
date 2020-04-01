#include <errno.h>
#include "choco.h"

inline void* aligned_malloc(size_t size, size_t align) {
  void* ptr = nullptr;
  int rc = posix_memalign(&ptr, align, size);
  return rc == 0 ? (errno = 0, ptr) : (errno = rc, nullptr);
}

inline void aligned_free(void* aligned_ptr) {
  free(aligned_ptr);
}


TypeInfo::TypeInfo(Type type, const char* name, size_t size, PrintDebugFunc print_func) :
_type(type),
_name(name),
_size(size),
_print_func(print_func) {
}


static void PrintNothing(const void* data, string * str) {
    str->append("NULL");
}

static void PrintInt8(const void* data, string * str) {
    char buff[32];
    snprintf(buff, 32, "%d", (int32_t)*(int8_t*)data);
    str->append(buff);
}

static void PrintInt16(const void* data, string * str) {
    char buff[32];
    snprintf(buff, 32, "%d", (int32_t)*(int16_t*)data);
    str->append(buff);
}

static void PrintInt32(const void* data, string * str) {
    char buff[32];
    snprintf(buff, 32, "%d", *(int32_t*)data);
    str->append(buff);
}

static void PrintInt64(const void* data, string * str) {
    char buff[32];
    snprintf(buff, 32, "%ld", *(int64_t*)data);
    str->append(buff);
}

static void PrintInt128(const void* data, string * str) {
    char buff[64];
    snprintf(buff, 64, "%016lx%016lx", *(uint64_t*)((uint8_t*)data+8), *(uint64_t*)(data));
    str->append(buff);
}

static void PrintFloat32(const void* data, string * str) {
    char buff[32];
    snprintf(buff, 32, "%f", *(float*)data);
    str->append(buff);
}

static void PrintFloat64(const void* data, string * str) {
    char buff[32];
    snprintf(buff, 32, "%lf", *(double*)data);
    str->append(buff);
}

static void PrintString(const void* data, string * str) {
    Slice* s = (Slice*)data;
    str->append((const char*)s->data(), s->size());
}


static TypeInfo gInfos[] = {
    TypeInfo(Type::Nothing, "Nothing", 0, PrintNothing),
    TypeInfo(Type::Int8, "Int8", 1, PrintInt8),
    TypeInfo(Type::Int16, "Int16", 2, PrintInt16),
    TypeInfo(Type::Int32, "Int32", 4, PrintInt32),
    TypeInfo(Type::Int64, "Int64", 8, PrintInt64),
    TypeInfo(Type::Int128, "Int128", 16, PrintInt128),
    TypeInfo(Type::Float32, "Float32", 4, PrintFloat32),
    TypeInfo(Type::Float64, "Float64", 8, PrintFloat64),
    TypeInfo(Type::String, "String", sizeof(Slice), PrintString),
};

const TypeInfo& TypeInfo::get(Type type) {
    if (type >= Type::Nothing && type <= Type::String) {
        return gInfos[type];
    }
    FATAL_EXIT
}


Variant::Variant(Type type, const void* value) : _type(Type::Nothing) {
    reset(type, value);
}

Variant::Variant(const string& str) {
    Slice slice(str);
    reset(Type::String, &slice);
}

Variant::Variant(const char *data, size_t size) {
    Slice slice(data, size);
    reset(Type::String, &slice);
}

void Variant::clear() {
    if (_type == Type::String) {
        Slice *str = (Slice*)&_value;
        if (str->size() > 0) {
            aligned_free(str->mutable_data());
            str->clear();
        }
    } else {
        _value.i128 = 0;
    }
}

void Variant::reset(Type type, const void* value) {
    assert(value!=NULL);
    if (_type != Type::Nothing) {
        clear();
    }
    _type = type;
    switch(type) {
    case Type::Nothing:
        FATAL_EXIT
    case Type::Int8:
        _value.i8 = *(int8_t*)value;
        break;
    case Type::Int16:
        _value.i16 = *(int16_t*)value;
        break;
    case Type::Int32:
        _value.i32 = *(int32_t*)value;
        break;
    case Type::Int64:
        _value.i64 = *(int64_t*)value;
        break;
    case Type::Int128:
        _value.i128 = *(__int128*)value;
        break;
    case Type::Float32:
        _value.f32 = *(float*)value;
        break;
    case Type::Float64:
        _value.f64 = *(double*)value;
        break;
    case Type::String:
        {
            const Slice *str = static_cast<const Slice *>(value);
            if (str->size() > 0) {
                char* blob = (char*)aligned_malloc(str->size(), 16);
                memcpy(blob, str->data(), str->size());
                *(Slice*)&_value = Slice(blob, str->size());
            }
        }
        break;
    default:
        FATAL_EXIT
    }
}


bool Variant::operator==(const Variant& rhs) const {
    if (_type != rhs._type) {
        return false;
    }
    switch (_type) {
    case Type::Nothing:
        FATAL_EXIT
    case Type::Int8:
        return _value.i8 == rhs._value.i8;
    case Type::Int16:
        return _value.i16 == rhs._value.i16;
    case Type::Int32:
        return _value.i32 == rhs._value.i32;
    case Type::Int64:
        return _value.i64 == rhs._value.i64;
    case Type::Int128:
        return _value.i128 == rhs._value.i128;
    case Type::Float32:
        return _value.f32 == rhs._value.f32;
    case Type::Float64:
        return _value.f64 == rhs._value.f64;
    case Type::String:
        return *(Slice*)&_value == *(Slice*)&rhs._value;
    default:
        FATAL_EXIT
    }
}



Schema::Schema(vector<ColumnSchema>& columns, uint32_t num_key_column) {
    if (num_key_column > columns.size()) {
        FATAL_EXIT
    }
    uint32_t max_cid = 0;
    columns.swap(_columns);
    _num_key_column = num_key_column;
    for (auto& c : _columns) {
        _name_to_col[c.name] = &c;
        _cid_to_col[c.cid] = &c;
        max_cid = std::max(c.cid, max_cid);
    }
    _next_cid = max_cid+1;
}

uint32_t Schema::next_cid() const {
    return _next_cid;
}

const ColumnSchema* Schema::get(const string& name) const {
    auto itr = _name_to_col.find(name);
    if (itr == _name_to_col.end()) {
        return nullptr;
    } else {
        return itr->second;
    }
}

const ColumnSchema* Schema::get(uint32_t cid) const {
    auto itr = _cid_to_col.find(cid);
    if (itr == _cid_to_col.end()) {
        return nullptr;
    } else {
        return itr->second;
    }
}

