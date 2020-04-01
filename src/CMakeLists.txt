cmake_minimum_required(VERSION 3.10)

project(choco CXX)

set(CMAKE_CXX_STANDARD 11)

#set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_BUILD_TYPE Release)

include_directories(.)

add_subdirectory(gutil)

add_library(choco
  choco.cpp
)

set(CHOCO_LINK_LIBS choco Gutil glog)

add_executable(choco_test
	choco_test.cpp
)
target_link_libraries(choco_test ${CHOCO_LINK_LIBS})
