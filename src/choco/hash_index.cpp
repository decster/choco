#include <emmintrin.h>
#include <stdio.h>
#include <algorithm>
#include "hash_index.h"
#include "common.h"

namespace choco {

using TagVector = __m128i;

static const bool kHashIndexStats = true;

struct alignas(64) HashChunk {
    static const uint32_t CAPACITY = 12;
    uint8_t tags[12];
    uint32_t size;
    uint32_t values[12];

    TagVector* tagVector() {
        return (TagVector*)tags;
    }

    void dump() {
        printf("[");
        for (uint32_t i=0;i<std::min(size, (uint32_t)12);i++) {
            printf("%6u(%02x)", values[i], (uint32_t)tags[i]);
        }
        printf("]\n");
    }
};


HashIndex::HashIndex(size_t capacity) :
        _size(0), _max_size(0), _num_chunks(0), _chunk_mask(0),
        _nfind(0), _nentry(0), _nprobe(0), _nset(0),
        _chunks(NULL) {
    size_t min_chunk = (capacity * 14 / 12 + HashChunk::CAPACITY - 1) / HashChunk::CAPACITY;
    if (min_chunk == 0) {
        return;
    }
    size_t nc = 1;
    while (nc < min_chunk) {
        nc *= 2;
    }
    _chunks = (HashChunk*)aligned_malloc(nc*64, 64);
    if (_chunks) {
        _num_chunks = nc;
        _chunk_mask = nc - 1;
        memset(_chunks, 0, _num_chunks*64);
        _max_size = this->capacity() * 12 / 14;
    }
}

HashIndex::~HashIndex() {
    if (_chunks) {
        aligned_free(_chunks);
        _chunks = 0;
        _size = 0;
        _max_size = 0;
        _num_chunks = 0;
        _chunk_mask = 0;
    }
}

size_t HashIndex::capacity() {
    return _num_chunks * HashChunk::CAPACITY;
}

uint32_t HashIndex::find(uint64_t keyHash, std::vector<Entry> &entries) {
    if (kHashIndexStats) _nfind++;
    uint64_t tag = keyHash & 0xff;
    if (tag == 0) {
        tag = 1;
    }
    uint64_t pos = (keyHash >> 8) & _chunk_mask;
    uint64_t orig_pos = pos;
    auto tests = _mm_set1_epi8(static_cast<uint8_t>(tag));
    while (true) {
        if (kHashIndexStats) _nprobe++;
        HashChunk& chunk = _chunks[pos];
        auto tags = _mm_load_si128(chunk.tagVector());
        auto eqs = _mm_cmpeq_epi8(tags, tests);
        uint32_t mask = _mm_movemask_epi8(eqs) & 0xfff;
        while (mask != 0) {
            uint32_t i = __builtin_ctz(mask);
            mask &= (mask -1);
            entries.emplace_back((pos << 4) | i, chunk.values[i]);
            if (kHashIndexStats) _nentry++;
        }
        if (chunk.size == HashChunk::CAPACITY) {
            uint64_t step =  tag*2+1; // 1;
            pos = (pos + step) & _chunk_mask;
            if (pos == orig_pos) {
                return NOSLOT;
            }
        } else {
            return (pos << 4) | chunk.size;
        }
    }
}

void HashIndex::set(uint32_t slot, uint64_t keyHash, uint32_t value) {
    if (kHashIndexStats) _nset++;
    uint32_t pos = slot >> 4;
    uint32_t tpos = slot & 0xf;
    HashChunk& chunk = _chunks[pos];
    uint64_t tag = keyHash & 0xff;
    if (tag == 0) {
        tag = 1;
    }
    chunk.tags[tpos] = tag;
    chunk.values[tpos] = value;
    if (tpos == chunk.size) {
        chunk.size = tpos + 1;
        _size++;
    }
}

bool HashIndex::add(uint64_t keyHash, uint32_t value) {
    if (kHashIndexStats) _nfind++;
    uint64_t tag = keyHash & 0xff;
    if (tag == 0) {
        tag = 1;
    }
    uint64_t pos = (keyHash >> 8) & _chunk_mask;
    uint64_t orig_pos = pos;
    while (true) {
        if (kHashIndexStats) _nprobe++;
        HashChunk& chunk = _chunks[pos];
        if (chunk.size == HashChunk::CAPACITY) {
            uint64_t step =  tag*2+1; // 1;
            pos = (pos + step) & _chunk_mask;
            if (pos == orig_pos) {
                return false;
            }
        } else {
            chunk.tags[chunk.size] = tag;
            chunk.values[chunk.size] = value;
            chunk.size++;
            _size++;
            return true;
        }
    }
}


void HashIndex::clear_stats() {
    _nfind = 0;
    _nentry = 0;
    _nprobe = 0;
    _nset = 0;
}

void HashIndex::dump() {
    if (_num_chunks <= 32) {
        for (size_t i=0;i<_num_chunks;i++) {
            auto& chunk = _chunks[i];
            printf("%3zu: ", i);
            chunk.dump();
        }
    }
    Log("chunk: %zu %.1fM capacity: %zu/%zu slot util: %.3f",
        _num_chunks, _num_chunks*64.0f/(1024*1024), size(), max_size(),
        size() / (_num_chunks*12.0f));
    if (kHashIndexStats) {
        Log("find: %zu entry: %zu(%.3f) probe: %zu(%.3f)", _nfind, _nentry, (float)_nentry/_nfind, _nprobe, (float)_nprobe/_nfind);
    }
}


}

