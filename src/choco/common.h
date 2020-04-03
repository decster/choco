#ifndef CHOCO_COMMON_H_
#define CHOCO_COMMON_H_

#include <stdio.h>
#include <unistd.h>
#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <memory>
#include <mutex>
#include <atomic>
#include "common/logging.h"
#include "status.h"
#include "slice.h"
#include "hashcode.h"

using std::vector;
using std::string;
using std::unique_ptr;
using std::shared_ptr;
using std::unordered_map;
using std::mutex;

namespace choco {

using std::string;

#if _POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600 ||   \
    (defined(__ANDROID__) && (__ANDROID_API__ > 16)) ||     \
    (defined(__APPLE__) &&                                  \
     (__MAC_OS_X_VERSION_MIN_REQUIRED >= __MAC_10_6 ||      \
      __IPHONE_OS_VERSION_MIN_REQUIRED >= __IPHONE_3_0)) || \
    defined(__FreeBSD__) || defined(__wasm32__)

inline void* aligned_malloc(size_t size, size_t align) {
  // use posix_memalign, but mimic the behaviour of memalign
  void* ptr = nullptr;
  int rc = posix_memalign(&ptr, align, size);
  return rc == 0 ? (errno = 0, ptr) : (errno = rc, nullptr);
}

inline void aligned_free(void* aligned_ptr) {
  free(aligned_ptr);
}

#elif defined(_WIN32)

inline void* aligned_malloc(size_t size, size_t align) {
  return _aligned_malloc(size, align);
}

inline void aligned_free(void* aligned_ptr) {
  _aligned_free(aligned_ptr);
}

#else

inline void* aligned_malloc(size_t size, size_t align) {
  return memalign(align, size);
}

inline void aligned_free(void* aligned_ptr) {
  free(aligned_ptr);
}

#endif

template<typename T, std::size_t Alignment=alignof(T)>
class aligned_allocator {
public:

    // The following will be the same for virtually all allocators.
    typedef T *pointer;
    typedef const T *const_pointer;
    typedef T &reference;
    typedef const T &const_reference;
    typedef T value_type;
    typedef std::size_t size_type;
    typedef ptrdiff_t difference_type;

    T* address(T &r) const {
        return &r;
    }

    const T* address(const T &s) const {
        return &s;
    }

    std::size_t max_size() const {
        // The following has been carefully written to be independent of
        // the definition of size_t and to avoid signed/unsigned warnings.
        return (static_cast<std::size_t>(0) - static_cast<std::size_t>(1))
                / sizeof(T);
    }

    // The following must be the same for all allocators.
    template<typename U>
    struct rebind {
        typedef aligned_allocator<U, Alignment> other;
    };

    bool operator!=(const aligned_allocator &other) const {
        return !(*this == other);
    }

    void construct(T *const p, const T &t) const {
        void *const pv = static_cast<void*>(p);

        new (pv) T(t);
    }

    void destroy(T *const p) const {
        p->~T();
    }

    // Returns true if and only if storage allocated from *this
    // can be deallocated from other, and vice versa.
    // Always returns true for stateless allocators.
    bool operator==(const aligned_allocator &other) const {
        return true;
    }

    // Default constructor, copy constructor, rebinding constructor, and destructor.
    // Empty for stateless allocators.
    aligned_allocator() {
    }

    aligned_allocator(const aligned_allocator&) {
    }

    template<typename U> aligned_allocator(
            const aligned_allocator<U, Alignment>&) {
    }

    ~aligned_allocator() {
    }

    // The following will be different for each allocator.
    T* allocate(const std::size_t n) const {
        // The return value of allocate(0) is unspecified.
        // Mallocator returns NULL in order to avoid depending
        // on malloc(0)'s implementation-defined behavior
        // (the implementation can define malloc(0) to return NULL,
        // in which case the bad_alloc check below would fire).
        // All allocators can return NULL in this case.
        if (n == 0) {
            return NULL;
        }

        // All allocators should contain an integer overflow check.
        // The Standardization Committee recommends that std::length_error
        // be thrown in the case of integer overflow.
        if (n > max_size()) {
            throw std::length_error(
                    "aligned_allocator<T>::allocate() - Integer overflow.");
        }

        // Mallocator wraps malloc().
        void *const pv = aligned_malloc(n * sizeof(T), Alignment);

        // Allocators should throw std::bad_alloc in the case of memory allocation failure.
        if (pv == NULL) {
            throw std::bad_alloc();
        }

        return static_cast<T*>(pv);
    }

    void deallocate(T *const p, const std::size_t n) const {
        aligned_free(p);
    }

    // The following will be the same for all allocators that ignore hints.
    template<typename U>
    T* allocate(const std::size_t n, const U* /* const hint */) const {
        return allocate(n);
    }

    // Allocators are not required to be assignable, so
    // all allocators should have a private unimplemented
    // assignment operator. Note that this will trigger the
    // off-by-default (enabled under /Wall) warning C4626
    // "assignment operator could not be generated because a
    // base class assignment operator is inaccessible" within
    // the STL headers, but that warning is useless.
private:
    aligned_allocator& operator=(const aligned_allocator&);
};



class RefCounted {
public:
    RefCounted() : _ref_count(1) {}

    size_t addRef() {
        return ++_ref_count;
    }
    size_t decRef() {
        return --_ref_count;
    }

private:
    std::atomic<size_t> _ref_count;
};


template <class T>
class RefPtr {
public:
    RefPtr() : _obj(nullptr) {}

    explicit RefPtr(T* obj, bool addref=true) : _obj(obj) {if(addref) addRef();}

    RefPtr(const RefPtr& rhs) : _obj(rhs._obj) {
        addRef();
    }

    RefPtr(RefPtr&& rhs) : _obj(rhs._obj) {
        rhs._obj = nullptr;
    }

    ~RefPtr() {
        decRef();
    }

    void swap(RefPtr& rhs) {
        std::swap(_obj, rhs._obj);
    }

    RefPtr& operator=(const RefPtr& rhs) {
        RefPtr tmp = rhs;
        tmp.swap(tmp);
        return *this;
    }

    T* get() const {
        return _obj;
    }

    T& operator*() const {
        return *_obj;
    }

    T* operator->() const {
        return _obj;
    }

    operator bool() const {
        return _obj != nullptr;
    }

    void reset() {
        decRef();
    }

private:
    void addRef() {
        if (_obj) {
            _obj->addRef();
        }
    }

    void decRef() {
        if (_obj) {
            if (_obj->decRef() == 0) {
                delete _obj;
            }
            _obj = nullptr;
        }
    }

    T* _obj;
};


string Format(const char * fmt, ...);

#define Logs(str) LOG(INFO) << str
#define Log(fmt, ...) LOG(INFO) << Format(fmt, __VA_ARGS__)

double Time();

}

#endif /* CHOCO_COMMON_H_ */
