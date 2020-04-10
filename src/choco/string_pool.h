#ifndef CHOCO_STRING_POOL_H_
#define CHOCO_STRING_POOL_H_

#include <endian.h>
#include "common.h"
#include "buffer.h"

namespace choco {

using std::string;

/**
 * Short string in string pool, 2 byte length(max 65535) followed by actual content,
 * 8 byte aligned, tailing unused space will be set to 0.
 */
struct alignas(8) SString {
    static SString Empty;
    static const size_t MaxLength = 1<<16;
    uint16_t len = 0;
    uint8_t  str[6] = {0,0,0,0,0,0};

    string to_string() const {
        return string((char*)str, (size_t)len);
    }

    bool operator==(const SString& rhs) const {
        uint64_t* cur = (uint64_t*)this;
        uint64_t* rcur = (uint64_t*)&rhs;
        if (*cur != *rcur) {
            return false;
        }
        uint64_t* end = (uint64_t*)(str + len);
        while (true) {
            if ((++cur) >= end) {
                return true;
            }
            if (*cur != *(++rcur)) {
                return false;
            }
        }
    }

    bool operator<(const SString& rhs) const {
        if (len < rhs.len) {
            return memcmp(str, rhs.str, len) <= 0;
        } else if (len == rhs.len) {
            return memcmp(str, rhs.str, len) < 0;
        } else {
            return memcmp(str, rhs.str, len) < 0;
        }
    }

    bool operator>(const SString& rhs) const {
        return rhs < *this;
    }

    bool operator<=(const SString& rhs) const {
        return !(*this > rhs);
    }

    bool operator>=(const SString& rhs) const {
        return !(*this < rhs);
    }

    int cmp(const SString& rhs) const {
        if (len < rhs.len) {
            int ret = memcmp(str, rhs.str, len);
            return ret == 0 ? -1 : ret;
        } else if (len == rhs.len) {
            return memcmp(str, rhs.str, len);
        } else {
            int ret = memcmp(str, rhs.str, len);
            return  ret == 0 ? 1 : ret;
        }
    }
};


/**
 * Dynamically allocated & managed SString
 */
class MString : public SString {
public:
    static MString* create(const Slice& str);
    ~MString();
private:
    MString() = default;
};


struct alignas(32) LString {
    uint32_t len;
    uint8_t  str[28];
};


class PoolSegment : public RefCounted {
public:
    PoolSegment() = default;
    ~PoolSegment() { aligned_free(buff); buff = nullptr; }

    Status init(size_t size);

    const SString* get(size_t offset) const;

    uint64_t pid = 0;
    uint8_t * buff = nullptr;
};

/**
 * Copy-on-Write append only string pool
 * stores 8-byte aligned SString
 */
class StringPool : public RefCounted {
public:
    static const size_t kSegmentSize = 1<<20; // 1M
    static const uint32_t NullId = 0;
    static const uint32_t EmptyId = 1;

    static Status create(RefPtr<StringPool>& pool);

    // get SString by sid
    const SString* get(uint32_t sid) const;

    // is's a static method, because this write op may expand the segment array and cause a COW
    // add not nullable slice, return string id(sid)
    static Status add(RefPtr<StringPool>& ref, const Slice& slice, uint32_t& sid);

    // is's a static method, because this write op may expand the segment array and cause a COW
    // add nullable slice, return string id(sid)
    static Status add(RefPtr<StringPool>& ref, const Slice* slice, uint32_t& sid) {
        if (!slice) {
            sid = NullId;
            return Status::OK();
        }
        return add(ref, *slice, sid);
    }

private:
    StringPool() = default;

    Status expand(RefPtr<StringPool>& cow_pool, size_t new_capacity=0);

    Status add_segment();

    Status add_unsafe(const Slice& slice, size_t storage_size, uint32_t& sid);

    vector<RefPtr<PoolSegment>> _segments;
    size_t _cur_seg_idx = 0;
    uint8_t* _cur_seg_base = nullptr;
    size_t _cur_len = 0;
};



} /* namespace choco */

#endif /* CHOCO_STRING_POOL_H_ */
