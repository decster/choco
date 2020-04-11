cmake_minimum_required(VERSION 3.10)

project(choco CXX)

set(CMAKE_CXX_STANDARD 11)

set(CMAKE_BUILD_TYPE Debug)
#set(CMAKE_BUILD_TYPE Release)

include_directories(.)

add_library(gtest STATIC
gtest/gtest-all.cc
)
target_include_directories(gtest PRIVATE gtest)

add_library(glog STATIC
glog/demangle.cc
glog/logging.cc
glog/raw_logging.cc
glog/signalhandler.cc
glog/symbolize.cc
glog/utilities.cc
glog/vlog_is_on.cc
)

add_subdirectory(gutil)
add_subdirectory(choco)

