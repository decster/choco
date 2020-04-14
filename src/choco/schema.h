#ifndef CHOCO_SCHEMA_H_
#define CHOCO_SCHEMA_H_

#include "common.h"
#include "type.h"

namespace choco {

struct ColumnSchema {
    string name;
    // column id, must starts with 1 (0 is special id reserved for delete flag column)
    uint32_t cid;
    Type type;
    bool nullable;
    unique_ptr<Variant> default_value;

    ColumnSchema(const string& name, uint32_t cid, Type type, bool nullable=false) :
        name(name),
        cid(cid),
        type(type),
        nullable(nullable) {}

    ColumnSchema(const ColumnSchema& rhs) {
        name = rhs.name;
        cid = rhs.cid;
        type = rhs.type;
        nullable = rhs.nullable;
        if (rhs.default_value) {
            default_value.reset(new Variant(*rhs.default_value));
        }
    }

    string to_string() const;

    const void * default_value_ptr() const {
        return default_value ? default_value->value() : nullptr;
    }

    static Status create(uint32_t cid, const Slice& desc, unique_ptr<ColumnSchema>& cs);
};


class Schema {
public:
    Schema(vector<ColumnSchema>& columns, uint32_t num_key_column);
    ~Schema();

    uint32_t cid_size() const;
    const ColumnSchema* get(const string& name) const;
    const ColumnSchema* get(uint32_t cid) const;
    const vector<ColumnSchema>& columns() const { return _columns; }
    uint32_t num_key_column() const { return _num_key_column; }

    static Status create(const Slice& desc, unique_ptr<Schema>& schema);

private:
    vector<ColumnSchema> _columns;
    uint32_t _num_key_column = 0;
    uint32_t _cid_size;
    unordered_map<string, ColumnSchema*> _name_to_col;
    vector<ColumnSchema*> _cid_to_col;
};


} /* namespace choco */

#endif /* CHOCO_SCHEMA_H_ */
