#ifndef SRC_STATUS_H_
#define SRC_STATUS_H_

#include <errno.h>
#include <stdint.h>
#include <string>

using std::string;

class Status {
public:
    Status() : _state(nullptr) {}
    ~Status() { delete [] _state; }

    operator bool() const { return _state == nullptr; }

    static Status OK() { return Status(); }

private:
    const char* _state;
};

#endif /* SRC_STATUS_H_ */
