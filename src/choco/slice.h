#ifndef SRC_SLICE_H_
#define SRC_SLICE_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <string>

using std::string;


class alignas(16) Slice {
 public:
    Slice() {}
  /// Create a slice that refers to a @c uint8_t byte array.
  ///
  /// @param [in] d
  ///   The input array.
  /// @param [in] n
  ///   Number of bytes in the array.
  Slice(const uint8_t* d, size_t n) : data_(d), size_(n) { }

  /// Create a slice that refers to a @c char byte array.
  ///
  /// @param [in] d
  ///   The input array.
  /// @param [in] n
  ///   Number of bytes in the array.
  Slice(const char* d, size_t n) :
    data_(reinterpret_cast<const uint8_t *>(d)),
    size_(n) { }

  /// Create a slice that refers to the contents of the given string.
  ///
  /// @param [in] s
  ///   The input string.
  Slice(const std::string& s) : // NOLINT(runtime/explicit)
    data_(reinterpret_cast<const uint8_t *>(s.data())),
    size_(s.size()) { }

  /// Create a slice that refers to a C-string s[0,strlen(s)-1].
  ///
  /// @param [in] s
  ///   The input C-string.
  Slice(const char* s) : // NOLINT(runtime/explicit)
    data_(reinterpret_cast<const uint8_t *>(s)),
    size_(strlen(s)) { }

  /// @return A pointer to the beginning of the referenced data.
  const uint8_t* data() const { return data_; }

  /// @return A mutable pointer to the beginning of the referenced data.
  uint8_t *mutable_data() { return const_cast<uint8_t *>(data_); }

  /// @return The length (in bytes) of the referenced data.
  size_t size() const { return size_; }

  /// @return @c true iff the length of the referenced data is zero.
  bool empty() const { return size_ == 0; }

  /// @pre n < size()
  ///
  /// @param [in] n
  ///   The index of the byte.
  /// @return the n-th byte in the referenced data.
  const uint8_t &operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  /// Change this slice to refer to an empty array.
  void clear() {
    data_ = reinterpret_cast<const uint8_t *>("");
    size_ = 0;
  }

  /// Drop the first "n" bytes from this slice.
  ///
  /// @pre n <= size()
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   Number of bytes that should be dropped from the beginning.
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  /// Truncate the slice to the given number of bytes.
  ///
  /// @pre n <= size()
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   The new size of the slice.
  void truncate(size_t n) {
    assert(n <= size());
    size_ = n;
  }


  /// @return A string that contains a copy of the referenced data.
  std::string ToString() const {
      return std::string(reinterpret_cast<const char *>(data_), size_);
  }

  /// Do a three-way comparison of the slice's data.
  ///
  /// @param [in] b
  ///   The other slice to compare with.
  /// @return Values are
  ///   @li <  0 iff "*this" <  "b"
  ///   @li == 0 iff "*this" == "b"
  ///   @li >  0 iff "*this" >  "b"
  int compare(const Slice& b) const {
      const int min_len = (size_ < b.size_) ? size_ : b.size_;
      int r = MemCompare(data_, b.data_, min_len);
      if (r == 0) {
        if (size_ < b.size_) r = -1;
        else if (size_ > b.size_) r = +1;
      }
      return r;
  }

  /// Check whether the slice starts with the given prefix.
  /// @param [in] x
  ///   The slice in question.
  /// @return @c true iff "x" is a prefix of "*this"
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (MemEqual(data_, x.data_, x.size_)));
  }

  /// @brief Comparator struct, useful for ordered collections (like STL maps).
  struct Comparator {
    /// Compare two slices using Slice::compare()
    ///
    /// @param [in] a
    ///   The slice to call Slice::compare() at.
    /// @param [in] b
    ///   The slice to use as a parameter for Slice::compare().
    /// @return @c true iff @c a is less than @c b by Slice::compare().
    bool operator()(const Slice& a, const Slice& b) const {
      return a.compare(b) < 0;
    }
  };

  /// Relocate/copy the slice's data into a new location.
  ///
  /// @param [in] d
  ///   The new location for the data. If it's the same location, then no
  ///   relocation is done. It is assumed that the new location is
  ///   large enough to fit the data.
  void relocate(uint8_t* d) {
    if (data_ != d) {
      memcpy(d, data_, size_);
      data_ = d;
    }
  }

 private:
  friend bool operator==(const Slice& x, const Slice& y);

  static bool MemEqual(const void* a, const void* b, size_t n) {
    return memcmp(a, b, n) == 0;
  }

  static int MemCompare(const void* a, const void* b, size_t n) {
    return memcmp(a, b, n);
  }

  const uint8_t* data_ = nullptr;
  size_t size_ = 0;
};


inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (Slice::MemEqual(x.data(), y.data(), x.size())));
}

inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

#endif /* SRC_SLICE_H_ */
