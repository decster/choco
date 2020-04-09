#ifndef CHOCO_SCHEMA_H_
#define CHOCO_SCHEMA_H_

#include "common.h"
#include "type.h"

namespace choco {

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

    const void * default_value_ptr() const {
        return default_value ? default_value->value() : nullptr;
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


} /* namespace choco */

#endif /* CHOCO_SCHEMA_H_ */
