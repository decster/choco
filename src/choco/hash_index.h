#ifndef HASH_INDEX_H_
#define HASH_INDEX_H_

#include <stdint.h>
#include <vector>
#include "utils.h"

namespace choco {

inline uint64_t Hash64(uint64_t key) {
    // twang_mix64
    key = (~key) + (key << 21); // key *= (1 << 21) - 1; key -= 1;
    key = key ^ (key >> 24);
    key = key + (key << 3) + (key << 8); // key *= 1 + (1 << 3) + (1 << 8)
    key = key ^ (key >> 14);
    key = key + (key << 2) + (key << 4); // key *= 1 + (1 << 2) + (1 << 4)
    key = key ^ (key >> 28);
    key = key + (key << 31); // key *= 1 + (1 << 31)
    return key;
}


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

#endif /* HASH_INDEX_H_ */
