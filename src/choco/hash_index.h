#ifndef CHOCO_HASH_INDEX_H_
#define CHOCO_HASH_INDEX_H_

#include <stdint.h>
#include <vector>

#include "common.h"

namespace choco {

struct HashChunk;

/**
 * Hash Index from hashcode -> row id(uint32)
 */
class HashIndex {
public:
    static const uint32_t NOSLOT = (uint32_t)-1;
    struct Entry {
        Entry(uint32_t slot, uint32_t value) : slot(slot), value(value) {}
        uint32_t slot;
        uint32_t value;
    };

    HashIndex(size_t capacity);
    ~HashIndex();

    size_t size() { return _size; }

    size_t max_size() { return _max_size; }

    size_t capacity();

    uint32_t find(uint64_t keyHash, std::vector<Entry> &entries);

    void set(uint32_t entry, uint64_t keyHash, uint32_t value);

    bool need_rehash() {
        return _size >= _max_size;
    }

    void clear_stats();

    void dump();

private:
    size_t _size;
    size_t _max_size;
    size_t _num_chunks;
    size_t _chunk_mask;
    size_t _nfind;
    size_t _nentry;
    size_t _nprobe;
    size_t _nset;
    HashChunk* _chunks;
};

}

#endif /* CHOCO_HASH_INDEX_H_ */
