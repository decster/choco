#ifndef CHOCO_STATUS_H_
#define CHOCO_STATUS_H_

#include <errno.h>
#include <stdint.h>
#include <string>
#include "gutil/port.h"
#include "slice.h"

using std::string;

namespace choco {

class Status {
public:
    Status() : _state(nullptr) {}
    ~Status() { delete [] _state; }

    Status(const Status& s);

    Status& operator=(const Status& s);

    Status(Status&& s);

    Status& operator=(Status&& s);

    operator bool() const { return _state == nullptr; }

    /// @return A string representation of this status suitable for printing.
    ///   Returns the string "OK" for success.
    std::string ToString() const;

    static Status OK() { return Status(); }

    static Status NotFound(const Slice& msg, const Slice& msg2 = Slice(),
                           int16_t posix_code = -1) {
      return Status(kNotFound, msg, msg2, posix_code);
    }

    static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice(),
                           int16_t posix_code = -1) {
      return Status(kNotSupported, msg, msg2, posix_code);
    }

    static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice(),
                           int16_t posix_code = -1) {
      return Status(kInvalidArgument, msg, msg2, posix_code);
    }

    static Status OOM(const Slice& msg, const Slice& msg2 = Slice(),
                           int16_t posix_code = -1) {
      return Status(kIOError, msg, msg2, posix_code);
    }

    static Status IOError(const Slice& msg, const Slice& msg2 = Slice(),
                           int16_t posix_code = -1) {
      return Status(kIOError, msg, msg2, posix_code);
    }

    /// @return @c true iff the status indicates a NotFound error.
    bool IsNotFound() const { return code() == kNotFound; }

    /// @return @c true iff the status indicates a NotSupported error.
    bool IsNotSupported() const { return code() == kNotSupported; }

    /// @return @c true iff the status indicates an IOError.
    bool IsIOError() const { return code() == kIOError; }

    /// @return @c true iff the status indicates an InvalidArgument error.
    bool IsInvalidArgument() const { return code() == kInvalidArgument; }

    std::string CodeAsString() const;

    /// This is similar to ToString, except that it does not include
    /// the stringified error code or POSIX code.
    ///
    /// @note The returned Slice is only valid as long as this Status object
    ///   remains live and unchanged.
    ///
    /// @return The message portion of the Status. For @c OK statuses,
    ///   this returns an empty string.
    Slice message() const;

    /// @return The POSIX code associated with this Status object,
    ///   or @c -1 if there is none.
    int16_t posix_code() const;

private:
    const char* _state;

    enum Code {
      kOk = 0,
      kNotFound = 1,
      kNotSupported = 2,
      kInvalidArgument = 3,
      kIOError = 4,
      kRuntimeError = 5,
      kIllegalState = 6,
      kOOM = 7,
    };

    Status(Code code, const Slice& msg, const Slice& msg2, int16_t posix_code);

    Code code() const {
      return (_state == NULL) ? kOk : static_cast<Code>(_state[4]);
    }

    static const char* CopyState(const char* s);
};

inline Status::Status(const Status& s) {
  _state = (s._state == NULL) ? NULL : CopyState(s._state);
}

inline Status& Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are OK.
  if (_state != s._state) {
    delete[] _state;
    _state = (s._state == NULL) ? NULL : CopyState(s._state);
  }
  return *this;
}


inline Status::Status(Status&& s) : _state(s._state) {
  s._state = nullptr;
}

inline Status& Status::operator=(Status&& s) {
  if (_state != s._state) {
    delete[] _state;
    _state = s._state;
    s._state = nullptr;
  }
  return *this;
}

#define RETURN_NOT_OK(s) do { \
    const Status& _s = (s);   \
    if (PREDICT_FALSE(!_s)) return _s;  \
  } while (0)
}

#endif /* SRC_STATUS_H_ */
