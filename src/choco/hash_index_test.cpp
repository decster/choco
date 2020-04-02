#include "gtest/gtest.h"
#include "choco/hash_index.h"

namespace choco {

TEST(hash_index, hash_index) {
    size_t sz = 20;
    HashIndex hi(sz);
    std::vector<HashIndex::Entry> entries;
    entries.reserve(10);
    Log("add %zu values, start", sz);
    // add all
    for (size_t i=0;i<sz*2;i+=2) {
        uint64_t keyHash = HashCode(i);
        entries.clear();
        uint32_t slot = hi.find(keyHash, entries);
        bool found = false;
        for (auto& e : entries) {
            uint64_t keyHashVerify = HashCode(e.value);
            if (keyHashVerify != keyHash) {
                continue;
            }
            if (e.value == i) {
                found = true;
                break;
            }
        }
        if (found || slot == HashIndex::NOSLOT) {
            Logs("Error!");
        }
        hi.set(slot, keyHash, i);
        //printf("add %9zu  slot %9u   size %9zu", i, slot, hi.size());
    }
    hi.dump();
    // search
    hi.clear_stats();
    Log("search %zu values, start", sz);
    for (size_t i=0;i<sz*2;i+=2) {
        uint64_t keyHash = HashCode(i);
        entries.clear();
        uint32_t slot = hi.find(keyHash, entries);
        uint32_t fslot = HashIndex::NOSLOT;
        for (auto& e : entries) {
            //printf("check entry: %u %u", e.slot, e.value);
            uint64_t keyHashVerify = HashCode(e.value);
            if (keyHashVerify != keyHash) {
                continue;
            }
            if (e.value == i) {
                fslot = e.slot;
                break;
            }
        }
        if (fslot == HashIndex::NOSLOT) {
            Log("find %9zu failed", i);
        } else if (entries.size() >= 5) {
            Log("more entries: %zu entry: %zu", i, entries.size());
        }
    }
    hi.dump();
}

}
