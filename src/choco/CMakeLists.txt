
add_library(choco STATIC
  status.cpp
  buffer.cpp
  column_delta.cpp
  column.cpp
  common.cpp
  hash_index.cpp
  mem_sub_tablet.cpp
  mem_tablet.cpp
  mem_tablet_get.cpp
  mem_tablet_scan.cpp
  mem_tablet_write.cpp
  schema.cpp
  type.cpp
)

set(CHOCO_LINK_LIBS choco Gutil glog pthread)

add_executable(choco_test
  choco_test.cpp
  hash_index_test.cpp
)
target_link_libraries(choco_test ${CHOCO_LINK_LIBS} gtest)

