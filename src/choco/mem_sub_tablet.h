#ifndef CHOCO_MEM_SUB_TABLET_H_
#define CHOCO_MEM_SUB_TABLET_H_

#include "common.h"
#include "hash_index.h"
#include "column.h"

namespace choco {

class PartialRowReader;

class MemSubTablet {
public:
	static Status create(uint64_t version, const Schema& schema, unique_ptr<MemSubTablet>& ret);

	~MemSubTablet();

    size_t latest_size() const { return _versions.back().size; }
    Status get_size(uint64_t version, size_t& size) const;
    Status read_column(uint64_t version, uint32_t cid, unique_ptr<ColumnReader>& reader);

    /**
     * caller should make sure schema valid during write
     */
    Status begin_write(const Schema& schema);
    Status apply_partial_row(const PartialRowReader& row);
    Status commit_write(uint64_t version);

private:
    DISALLOW_COPY_AND_ASSIGN(MemSubTablet);

    MemSubTablet();
    Status prepare_writer_for_column(uint32_t cid);
    RefPtr<HashIndex> rebuild_hash_index(size_t new_capacity);

    mutable mutex _lock;
    RefPtr<HashIndex> _index;
    struct VersionInfo {
    	VersionInfo(uint64_t version, uint64_t size) : version(version), size(size) {}
        uint64_t version=0;
        uint64_t size=0;
    };
    vector<VersionInfo> _versions;
    // cid -> column
    vector<RefPtr<Column>> _columns;

    // write state
    const Schema* _schema = nullptr;
    size_t _row_size = 0;
    RefPtr<HashIndex> _write_index;
    // cid -> current writers
    vector<unique_ptr<ColumnWriter>> _writers;
    // store temp entries
    std::vector<HashIndex::Entry> _temp_hash_entries;
};


} /* namespace choco */

#endif /* CHOCO_MEM_SUB_TABLET_H_ */
