#include "string_pool.h"

namespace choco {

SString SString::Empty;

//////////////////////////////////////////////////////////////////////////////

MString* MString::create(const Slice& str) {
    if (str.size() == 0) {
        return (MString*)&Empty;
    }
    CHECK_LT(str.size(), MaxLength);
    size_t slen = Padding(str.size()+2, 8);
    MString* ret = (MString*)aligned_malloc(slen, 8);
    if (!ret) {
        return nullptr;
    }
    ret->len = str.size();
    *(uint64_t*)((uint8_t*)ret + slen - 8) = 0;
    memcpy(ret->str, str.data(), str.size());
    return ret;
}

MString::~MString() {
    aligned_free(this);
}

//////////////////////////////////////////////////////////////////////////////

Status PoolSegment::init(size_t size) {
    buff = (uint8_t*)aligned_malloc(size, 4096);
    if (!buff) {
        return Status::OOM("allocate StringPool segment");
    }
    return Status::OK();
}

const SString* PoolSegment::get(size_t offset) const {
    DCHECK_NOTNULL(buff);
    DCHECK_LT(offset, StringPool::kSegmentSize);
    return (const SString*)(buff + offset);
}

//////////////////////////////////////////////////////////////////////////////

Status StringPool::create(RefPtr<StringPool>& pool) {
    RefPtr<PoolSegment> seg = RefPtr<PoolSegment>::create();
    RETURN_NOT_OK(seg->init(kSegmentSize));
    RefPtr<StringPool> ret(new StringPool(), false);
    ret->_segments.reserve(8);
    ret->_segments.emplace_back(std::move(seg));
    ret->_cur_seg_base = ret->_segments[0]->buff;
    // init 0(null) and 1(empty)
    memset(ret->_cur_seg_base, 0, 16);
    ret->_cur_len = 16;
    pool.swap(ret);
    return Status::OK();
}


// get SString by sid
const SString* StringPool::get(uint32_t sid) const {
    if (sid == 0) {
        return nullptr;
    }
    size_t offset = sid * 8;
    size_t segidx = offset / kSegmentSize;
    DCHECK_LT(segidx, _segments.size());
    size_t segoffset = offset & (kSegmentSize-1);
    return _segments[segidx]->get(segoffset);
}

Status StringPool::add_segment() {
    CHECK_LT(_segments.size(), _segments.capacity());
    RefPtr<PoolSegment> seg = RefPtr<PoolSegment>::create();
    RETURN_NOT_OK(seg->init(kSegmentSize));
    _cur_seg_idx++;
    _cur_seg_base = seg->buff;
    _cur_len = 0;
    _segments.emplace_back(std::move(seg));
    return Status::OK();
}


// add not nullable slice, return string id(sid)
Status StringPool::add(RefPtr<StringPool>& ref, const Slice& slice, uint32_t& sid) {
    if (slice.size() == 0) {
        sid = EmptyId;
        return Status::OK();
    }
    DCHECK_LT(slice.size(), 1<<16);
    size_t storage_size = Padding(slice.size() + 2, 8);
    if (ref->_cur_len + storage_size > kSegmentSize) {
        // try allocate new segments
        if (ref->_segments.size() == ref->_segments.capacity()) {
            RefPtr<StringPool> cow;
            RETURN_NOT_OK(ref->expand(cow));
            ref.swap(cow);
        }
        RETURN_NOT_OK(ref->add_segment());
    }
    return ref->add_unsafe(slice, storage_size, sid);
}


Status StringPool::add_unsafe(const Slice& slice, size_t storage_size, uint32_t& sid) {
    size_t old_len = _cur_len;
    _cur_len += storage_size;
    DCHECK_LE(_cur_len, kSegmentSize);
    // zero tailing blank space
    if (slice.size() + 2 < storage_size) {
        *(uint64_t*)(_cur_seg_base + _cur_len - 8)  = 0;
    }
    *(uint16_t*)(_cur_seg_base + old_len) = slice.size();
    // TODO: optimize fast memcpy
    memcpy(_cur_seg_base + old_len + 2, slice.data(), slice.size());
    sid = (_cur_seg_idx * kSegmentSize + old_len) / 8;
    return Status::OK();
}

Status StringPool::expand(RefPtr<StringPool>& cow_pool, size_t new_capacity) {
    if (new_capacity == 0) {
        size_t added = std::min((size_t)128, _segments.capacity());
        new_capacity = Padding(_segments.capacity() + added, 8);
    }
    // check pool doesn't need expanding
    DCHECK_EQ(_segments.size(), _segments.capacity());
    DCHECK(_segments.capacity() < new_capacity);
    RefPtr<StringPool> ret(new StringPool(), false);
    ret->_segments.reserve(new_capacity);
    ret->_segments.resize(_segments.size());
    for (size_t i=0;i<_segments.size();i++) {
        ret->_segments[i] = _segments[i];
    }
    ret->_cur_seg_idx = _cur_seg_idx;
    ret->_cur_seg_base = _cur_seg_base;
    ret->_cur_len = _cur_len;
    cow_pool.swap(ret);
    return Status::OK();
}

} /* namespace choco */
