add_library(choco STATIC
  buffer.cpp
  column_delta.cpp
  column.cpp
  common.cpp
  hash_index.cpp
  mem_sub_tablet.cpp
  mem_tablet.cpp
  mem_tablet_get.cpp
  mem_tablet_scan.cpp
  mem_tablet_writer.cpp
  schema.cpp
  slice.cpp
  status.cpp
  string_pool.cpp
  type.cpp
)

set(CHOCO_LINK_LIBS choco Gutil glog flatbuffers pthread)

add_executable(choco_test
  choco_test.cpp
  hash_index_test.cpp
  column_delta_test.cpp
  column_test.cpp
  mem_tablet_test.cpp
  schema_test.cpp
  slice_test.cpp
  string_pool_test.cpp
)
target_link_libraries(choco_test ${CHOCO_LINK_LIBS} gtest)

