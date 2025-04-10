cmake_minimum_required(VERSION 3.12)
project(hudi_cpp_example)

set(CMAKE_CXX_STANDARD 14)

# Find the hudi-cpp lib
execute_process(
    COMMAND cargo metadata --format-version=1
    OUTPUT_VARIABLE CARGO_METADATA
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../..
)
string(JSON CARGO_WORKSPACE_ROOT GET ${CARGO_METADATA} "workspace_root")
string(JSON TARGET_DIRECTORY GET ${CARGO_METADATA} "target_directory")

# Get generated C++ code location
set(CXX_GENERATED_DIR "${TARGET_DIRECTORY}/cxxbridge/hudi/src")

# Include directories
include_directories(${CARGO_WORKSPACE_ROOT}/cpp/include)
include_directories(${TARGET_DIRECTORY}/cxxbridge)

# Add the example executable and generated code
add_executable(hudi_cpp_example
    example.cpp
    ${CXX_GENERATED_DIR}/bridge.rs.cc
)

# Link with the Rust library
if(WIN32)
    target_link_libraries(hudi_cpp_example ${TARGET_DIRECTORY}/debug/hudi.lib)
elseif(APPLE)
    target_link_libraries(hudi_cpp_example ${TARGET_DIRECTORY}/debug/libhudi.dylib)
else()
    target_link_libraries(hudi_cpp_example ${TARGET_DIRECTORY}/debug/libhudi.so)
endif()

# Add a custom command to build the Rust library
if(WIN32)
    set(RUST_LIB_PATH ${TARGET_DIRECTORY}/debug/hudi.lib)
elseif(APPLE)
    set(RUST_LIB_PATH ${TARGET_DIRECTORY}/debug/libhudi.dylib)
else()
    set(RUST_LIB_PATH ${TARGET_DIRECTORY}/debug/libhudi.so)
endif()

add_custom_command(
    OUTPUT ${RUST_LIB_PATH}
    COMMAND cargo build
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/..
    COMMENT "Building Rust library"
)

# Add a custom target that depends on the Rust library
add_custom_target(
    rust_lib ALL
    DEPENDS ${RUST_LIB_PATH}
)
add_dependencies(hudi_cpp_example rust_lib)