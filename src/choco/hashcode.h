#ifndef CHOCO_HASH_H_
#define CHOCO_HASH_H_

#include "type.h"
#include "slice.h"

namespace choco {

template<class T>
uint64_t HashCode(const T value) {
    uint64_t key = static_cast<uint64_t>(value);
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

inline uint64_t HashCode(int128_t key) {
    // TODO:
    return 0;
}

inline uint64_t HashCode(const Slice& key) {
    // TODO:
    return 0;
}

}

#endif /* CHOCO_HASH_H_ */
