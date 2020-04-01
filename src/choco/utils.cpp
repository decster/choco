#include <stdlib.h>
#include <time.h>
#include <stdarg.h>
#include <inttypes.h>
#include "utils.h"

namespace choco {

void Log(const char * fmt, ...) {
    va_list arglist;
    va_start(arglist, fmt);
    vfprintf(stderr, fmt, arglist);
    va_end(arglist);
    fputc('\n', stderr);
}

string Format(const char * fmt, ...) {
    char buff[2048];
    va_list arglist;
    va_start(arglist, fmt);
    vsnprintf(buff, 2047, fmt, arglist);
    va_end(arglist);
    return buff;
}

double Time() {
    timespec spec;
    clock_gettime(CLOCK_REALTIME, &spec);
    return spec.tv_sec + spec.tv_nsec / 1000000000.0;
}

}
