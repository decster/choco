set(FlatBuffers_Library_SRCS
  code_generators.h
  base.h
  flatbuffers.h
  hash.h
  idl.h
  util.h
  reflection.h
  reflection_generated.h
  stl_emulation.h
  flexbuffers.h
  registry.h
  minireflect.h
  code_generators.cpp
  idl_parser.cpp
  idl_gen_text.cpp
  reflection.cpp
  util.cpp
)

set(FlatBuffers_Compiler_SRCS
  ${FlatBuffers_Library_SRCS}
  idl_gen_cpp.cpp
  idl_gen_dart.cpp
  idl_gen_general.cpp
  idl_gen_go.cpp
  idl_gen_js.cpp
  idl_gen_php.cpp
  idl_gen_python.cpp
  idl_gen_lobster.cpp
  idl_gen_lua.cpp
  idl_gen_rust.cpp
  idl_gen_fbs.cpp
  idl_gen_json_schema.cpp
  flatc.cpp
  flatc_main.cpp
)

add_library(flatbuffers STATIC ${FlatBuffers_Library_SRCS})

add_executable(flatc ${FlatBuffers_Compiler_SRCS})
if(NOT FLATBUFFERS_FLATC_EXECUTABLE)
  set(FLATBUFFERS_FLATC_EXECUTABLE $<TARGET_FILE:flatc>)
endif()

function(compile_flatbuffers_schema_to_cpp SRC_FBS)
  get_filename_component(SRC_FBS_DIR ${SRC_FBS} PATH)
  string(REGEX REPLACE "\\.fbs$" "_generated.h" GEN_HEADER ${SRC_FBS})
  add_custom_command(
    OUTPUT ${GEN_HEADER}
    COMMAND "${FLATBUFFERS_FLATC_EXECUTABLE}" -c --no-includes --gen-mutable
            --gen-object-api --gen-compare -o "${SRC_FBS_DIR}"
            --reflect-names
            -I "${CMAKE_CURRENT_SOURCE_DIR}/tests/include_test"
            "${CMAKE_CURRENT_SOURCE_DIR}/${SRC_FBS}"
    DEPENDS flatc)
endfunction()
