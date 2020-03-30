cmake_minimum_required(VERSION 3.10)

project(htdb CXX)

set(CMAKE_CXX_STANDARD 11)

#set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_BUILD_TYPE Release)

add_library(choco
  choco.cpp
)

add_executable(choco_test
	choco_test.cpp
)
target_link_libraries(choco_test choco)
