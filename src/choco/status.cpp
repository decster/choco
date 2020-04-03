#include "common.h"
#include "status.h"

namespace choco {

const char* Status::CopyState(const char* state) {
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  auto result = new char[size + 7];
  memcpy(result, state, size + 7);
  return result;
}

Status::Status(Code code, const Slice& msg, const Slice& msg2,
               int16_t posix_code) {
  DCHECK(code != kOk);
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0);
  auto result = new char[size + 7];
  memcpy(result, &size, sizeof(size));
  result[4] = static_cast<char>(code);
  memcpy(result + 5, &posix_code, sizeof(posix_code));
  memcpy(result + 7, msg.data(), len1);
  if (len2) {
    result[7 + len1] = ':';
    result[8 + len1] = ' ';
    memcpy(result + 9 + len1, msg2.data(), len2);
  }
  _state = result;
}

std::string Status::CodeAsString() const {
  if (_state == nullptr) {
    return "OK";
  }

  const char* type;
  switch (code()) {
    case kOk:
      type = "OK";
      break;
    case kNotFound:
      type = "Not found";
      break;
    case kNotSupported:
      type = "Not implemented";
      break;
    case kInvalidArgument:
      type = "Invalid argument";
      break;
    case kIOError:
      type = "IO error";
      break;
    case kRuntimeError:
      type = "Runtime error";
      break;
    case kIllegalState:
      type = "Illegal state";
      break;
    default:
      LOG(FATAL) << "unreachable";
  }
  return std::string(type);
}

std::string Status::ToString() const {
  std::string result(CodeAsString());
  if (_state == nullptr) {
    return result;
  }

  result.append(": ");
  Slice msg = message();
  result.append(reinterpret_cast<const char*>(msg.data()), msg.size());
  int16_t posix = posix_code();
  if (posix != -1) {
    char buf[64];
    snprintf(buf, sizeof(buf), " (error %d)", posix);
    result.append(buf);
  }
  return result;
}

Slice Status::message() const {
  if (_state == nullptr) {
    return Slice();
  }

  uint32_t length;
  memcpy(&length, _state, sizeof(length));
  return Slice(_state + 7, length);
}

int16_t Status::posix_code() const {
  if (_state == nullptr) {
    return 0;
  }
  int16_t posix_code;
  memcpy(&posix_code, _state + 5, sizeof(posix_code));
  return posix_code;
}

}

